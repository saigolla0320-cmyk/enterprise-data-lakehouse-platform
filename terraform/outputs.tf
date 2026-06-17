output "resource_group_name" {
  description = "Name of the created resource group"
  value       = azurerm_resource_group.lakehouse.name
}

output "adls_account_name" {
  description = "ADLS Gen2 storage account name"
  value       = azurerm_storage_account.adls.name
}

output "adls_primary_dfs_endpoint" {
  description = "Primary DFS endpoint for the data lake"
  value       = azurerm_storage_account.adls.primary_dfs_endpoint
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = azurerm_databricks_workspace.lakehouse.workspace_url
}

output "medallion_containers" {
  description = "Created medallion layer containers"
  value       = [for c in azurerm_storage_container.layers : c.name]
}

output "key_vault_uri" {
  description = "Key Vault URI for secret references"
  value       = azurerm_key_vault.lakehouse.vault_uri
}
