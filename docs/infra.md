# Infraestructura propuesta

La implementación de esta prueba se ejecuta localmente utilizando PySpark y Delta Lake.

Para un escenario productivo en Azure, la solución puede desplegarse utilizando:

- Azure Databricks
- Azure Data Lake Storage Gen2
- Unity Catalog
- Databricks Workflows
- Azure Key Vault
- Terraform

El objetivo del siguiente ejemplo es mostrar cómo podría automatizarse parte del aprovisionamiento. No forma parte de la ejecución local de la prueba.

## Organización

Se propone un catálogo por ambiente:

```text
mobility_dev
mobility_qa
mobility_main
```

Dentro de cada ambiente, los tenants se mantienen aislados lógicamente por schemas y físicamente mediante ubicaciones controladas en ADLS Gen2.

## Ejemplo Terraform

```hcl
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "location" {
  type    = string
  default = "eastus"
}

resource "azurerm_resource_group" "data" {
  name     = "rg-mobility-${var.environment}"
  location = var.location
}

resource "azurerm_storage_account" "datalake" {
  name                     = "stmobility${var.environment}"
  resource_group_name      = azurerm_resource_group.data.name
  location                 = azurerm_resource_group.data.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

resource "azurerm_storage_data_lake_gen2_filesystem" "lakehouse" {
  name               = "lakehouse"
  storage_account_id = azurerm_storage_account.datalake.id
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "filesystem_name" {
  value = azurerm_storage_data_lake_gen2_filesystem.lakehouse.name
}
```

## Onboarding mediante infraestructura

En producción, el onboarding de un tenant debería automatizar:

1. creación de schemas y rutas;
2. permisos del tenant;
3. configuración del pipeline;
4. validaciones de acceso;
5. ejecución inicial controlada.

El código PySpark permanecería común para todos los tenants. Las diferencias entre ambientes y tenants se resolverían mediante configuración.

## Orquestación

En producción utilizaría Databricks Workflows/Jobs para invocar el mismo entry point parametrizado utilizado en la solución.

Los parámetros principales serían:

- ambiente;
- tenant;
- fecha inicial;
- fecha final.

Esto permite mantener una única implementación y reutilizarla para múltiples tenants y ambientes.