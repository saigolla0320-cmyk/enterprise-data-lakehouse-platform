terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
  }

  backend "azurerm" {
    # Configure via -backend-config or CI secrets
    # resource_group_name  = "tfstate-rg"
    # storage_account_name = "tfstatelakehouse"
    # container_name       = "tfstate"
    # key                  = "lakehouse.terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "lakehouse" {
  name     = "${var.project_name}-${var.environment}-rg"
  location = var.location
  tags     = local.common_tags
}

# ── ADLS Gen2 — the data lake storage ──────────────────────────────────────
resource "azurerm_storage_account" "adls" {
  name                     = "${var.project_name}${var.environment}adls"
  resource_group_name      = azurerm_resource_group.lakehouse.name
  location                 = azurerm_resource_group.lakehouse.location
  account_tier             = "Standard"
  account_replication_type = "ZRS"
  is_hns_enabled           = true # Hierarchical namespace = ADLS Gen2
  min_tls_version          = "TLS1_2"
  tags                     = local.common_tags
}

# Medallion layer containers
resource "azurerm_storage_container" "layers" {
  for_each              = toset(["bronze", "silver", "gold"])
  name                  = each.key
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

# ── Databricks workspace ────────────────────────────────────────────────────
resource "azurerm_databricks_workspace" "lakehouse" {
  name                = "${var.project_name}-${var.environment}-dbx"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = azurerm_resource_group.lakehouse.location
  sku                 = var.environment == "prod" ? "premium" : "standard"
  tags                = local.common_tags
}

# ── Key Vault for secrets ───────────────────────────────────────────────────
resource "azurerm_key_vault" "lakehouse" {
  name                = "${var.project_name}-${var.environment}-kv"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = azurerm_resource_group.lakehouse.location
  tenant_id           = var.tenant_id
  sku_name            = "standard"
  tags                = local.common_tags
}

locals {
  common_tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
    owner       = "data-engineering"
  }
}
