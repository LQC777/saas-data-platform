# Observaciones técnicas

## 1. Aislamiento multi-tenant

La solución implementa aislamiento físico por tenant mediante rutas independientes:

- `data/bronze/<tenant>/`
- `data/silver/<tenant>/`
- `data/gold/<tenant>/`
- `data/silver_quarantine/<tenant>/`

Esto evita depender únicamente de filtros por `tenant_id` y reduce el riesgo de mezclar información entre países.

En un entorno productivo con Databricks y Unity Catalog, este modelo puede evolucionar a un catálogo por ambiente y schemas separados por tenant y capa.

## 2. Idempotencia por capa

La estrategia de idempotencia depende de la naturaleza de cada capa.

### Bronze

Bronze utiliza `overwrite` sobre el rango de fechas procesado. Esto permite reprocesar una ventana sin duplicar información.

### Silver

`fact_deliveries` utiliza Delta Lake `MERGE INTO` con la llave de negocio:

`tenant_id + fecha_proceso + transporte + ruta + material + tipo_entrega`

De esta forma, una nueva ejecución actualiza registros existentes o inserta registros nuevos.

### Gold

Gold es una capa derivada, por lo que las métricas de cada rango se recalculan desde Silver y se sobrescriben.

## 3. Tratamiento de anomalías

No todas las anomalías reciben el mismo tratamiento.

Los registros con fecha inválida, cantidad nula/no positiva, precio nulo o material inexistente en catálogo son enviados a quarantine con `_quarantine_reason`.

Los tipos de entrega fuera del alcance analítico (`COBR`, `Z99`) son descartados y contabilizados, pero no son persistidos en quarantine.

Los duplicados exactos se eliminan conservando un único registro.

## 4. Dimensión de materiales SCD Type 2

`dim_materials` conserva el historial de cambios mediante:

- `valid_from`
- `valid_to`
- `is_current`

La llave del MERGE es `material + valid_from`.

El enriquecimiento de `fact_deliveries` utiliza un join temporal entre `fecha_proceso`, `valid_from` y `valid_to`.

Esto evita enriquecer registros históricos utilizando solamente la versión actual del material.

## 5. Cálculo de revenue

`total_revenue` utiliza el precio de la transacción contenido en `fact_deliveries`:

`cantidad_normalizada_st * precio`

El campo `precio_base` del catálogo se utiliza únicamente como información descriptiva de la dimensión y no para calcular revenue.

## 6. Observabilidad

Cada ejecución de Data Quality registra sus resultados en:

`data/shared/quality_logs`

Cada validación contiene identificadores de ejecución, tenant, tabla, severidad, registros revisados, registros fallidos y resultado del check.

Esto permite conservar un historial básico de calidad y observabilidad del pipeline.