"""
Multi-Source Ingestion — Bronze Layer
Azure Data Factory-style ingestion framework. Pulls data from multiple
enterprise sources (REST APIs, databases, flat files) and lands raw data
into the Bronze layer of ADLS (simulated locally as Delta tables).
"""

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("ingestion")

BRONZE_ROOT = Path(__file__).resolve().parents[1] / "data" / "bronze"


@dataclass
class IngestionResult:
    source_name: str
    row_count: int
    landed_path: str
    ingested_at: str
    status: str


class BaseIngestor(ABC):
    """Abstract base for all source ingestors. Mirrors ADF linked-service pattern."""

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.bronze_path = BRONZE_ROOT / source_name
        self.bronze_path.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Pull raw data from the source."""
        ...

    def land(self, df: pd.DataFrame) -> IngestionResult:
        """Write raw data to bronze layer with ingestion metadata."""
        df["_ingested_at"] = datetime.utcnow().isoformat()
        df["_source_system"] = self.source_name

        partition = datetime.utcnow().strftime("%Y-%m-%d")
        dest = self.bronze_path / f"ingest_date={partition}"
        dest.mkdir(parents=True, exist_ok=True)
        file_path = dest / f"{self.source_name}.parquet"

        df.to_parquet(file_path, index=False)
        logger.info(f"[{self.source_name}] landed {len(df):,} rows → {file_path}")

        return IngestionResult(
            source_name=self.source_name,
            row_count=len(df),
            landed_path=str(file_path),
            ingested_at=datetime.utcnow().isoformat(),
            status="success",
        )

    def run(self) -> IngestionResult:
        logger.info(f"[{self.source_name}] starting ingestion")
        df = self.extract()
        return self.land(df)


class RestApiIngestor(BaseIngestor):
    """Ingests data from a paginated REST API."""

    def __init__(self, source_name: str, base_url: str, params: Optional[dict] = None):
        super().__init__(source_name)
        self.base_url = base_url
        self.params = params or {}

    def extract(self) -> pd.DataFrame:
        logger.info(f"[{self.source_name}] calling {self.base_url}")
        resp = requests.get(self.base_url, params=self.params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        records = data if isinstance(data, list) else data.get("results", [data])
        return pd.json_normalize(records)


class DatabaseIngestor(BaseIngestor):
    """Ingests data from a relational database via SQLAlchemy connection string."""

    def __init__(self, source_name: str, connection_string: str, query: str):
        super().__init__(source_name)
        self.connection_string = connection_string
        self.query = query

    def extract(self) -> pd.DataFrame:
        from sqlalchemy import create_engine
        logger.info(f"[{self.source_name}] executing query against database")
        engine = create_engine(self.connection_string)
        with engine.connect() as conn:
            return pd.read_sql(self.query, conn)


class FlatFileIngestor(BaseIngestor):
    """Ingests CSV / JSON flat files from a landing directory."""

    def __init__(self, source_name: str, file_path: str, file_format: str = "csv"):
        super().__init__(source_name)
        self.file_path = file_path
        self.file_format = file_format

    def extract(self) -> pd.DataFrame:
        logger.info(f"[{self.source_name}] reading {self.file_format} file {self.file_path}")
        if self.file_format == "csv":
            return pd.read_csv(self.file_path)
        elif self.file_format == "json":
            return pd.read_json(self.file_path)
        raise ValueError(f"Unsupported format: {self.file_format}")


def load_source_config() -> dict:
    """Load source definitions from config/sources.json."""
    config_path = Path(__file__).resolve().parents[1] / "config" / "sources.json"
    with open(config_path) as f:
        return json.load(f)


def run_all():
    """Orchestrate ingestion across all configured sources."""
    config = load_source_config()
    results = []

    for source in config["sources"]:
        try:
            stype = source["type"]
            if stype == "rest_api":
                ingestor = RestApiIngestor(source["name"], source["url"], source.get("params"))
            elif stype == "database":
                ingestor = DatabaseIngestor(source["name"], source["connection_string"], source["query"])
            elif stype == "flat_file":
                ingestor = FlatFileIngestor(source["name"], source["path"], source.get("format", "csv"))
            else:
                logger.warning(f"Unknown source type: {stype}, skipping {source['name']}")
                continue

            results.append(ingestor.run())
        except Exception as e:
            logger.error(f"[{source['name']}] ingestion failed: {e}")
            results.append(IngestionResult(source["name"], 0, "", datetime.utcnow().isoformat(), "failed"))

    success = sum(1 for r in results if r.status == "success")
    logger.info(f"Ingestion complete: {success}/{len(results)} sources succeeded")
    return results


if __name__ == "__main__":
    run_all()
