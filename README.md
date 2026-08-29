# SAAS Data Platform

Solución técnica orientada a una arquitectura de datos multi-tenant reproducible y mantenible.

Pipeline de datos multi-tenant desarrollado con PySpark, Delta Lake y OmegaConf para procesar información de entregas de productos utilizando una arquitectura Medallion.

La solución soporta los tenants:

- SV
- HN
- GT
- PE
- EC
- JM

## Tecnologías

- Python 3.11+
- PySpark 3.5.x
- Delta Lake 3.x
- OmegaConf
- Pytest
- Ruff
- GitHub Actions

## Arquitectura

El pipeline implementa las siguientes capas:

```text
CSV RAW
   |
   v
Bronze
   |
   v
Silver --------> Quarantine
   |
   v
Data Quality
   |
   v
Gold
```

### Bronze

La capa Bronze:

- lee los archivos CSV de origen;
- conserva el esquema original;
- agrega columnas técnicas;
- filtra la información correspondiente al tenant;
- persiste la información en Delta Lake;
- permite reprocesar rangos de fechas de forma idempotente.

Columnas técnicas principales:

```text
_tenant_id
_ingestion_timestamp
_source_file
_batch_id
```

### Silver

La capa Silver realiza:

- eliminación de duplicados exactos;
- validación de anomalías;
- normalización de unidades;
- filtrado de tipos de entrega;
- generación de flags de negocio;
- enriquecimiento con el catálogo de materiales;
- persistencia mediante Delta Lake MERGE.

Los tipos de entrega válidos son:

```text
ZPRE
ZVE1
Z04
Z05
```

La conversión de unidades utilizada es:

```text
1 CS = 20 ST
```

Los registros `ZPRE` y `ZVE1` son considerados entregas rutinarias.

Los registros `Z04` y `Z05` son considerados entregas bonus.

### Quarantine

Los registros inválidos se almacenan separadamente utilizando `_quarantine_reason`.

Se envían a quarantine casos como:

- fecha inválida;
- cantidad nula;
- cantidad menor o igual a cero;
- precio nulo;
- material inexistente en catálogo.

Los tipos de entrega fuera del alcance analítico, como `COBR` y `Z99`, se contabilizan pero no se persisten en quarantine.

### Dimensión de materiales

`dim_materials` implementa Slowly Changing Dimension Type 2.

La dimensión conserva:

```text
valid_from
valid_to
is_current
```

El enriquecimiento del fact utiliza un join temporal entre `fecha_proceso`, `valid_from` y `valid_to`.

Esto permite utilizar la versión del material que era válida en la fecha de la transacción.

### Data Quality

El pipeline ejecuta validaciones sobre Silver antes de generar Gold.

Entre los checks implementados se encuentran:

- unicidad de la llave de negocio;
- unidad normalizada igual a ST;
- cantidad normalizada positiva;
- material correctamente enriquecido;
- fecha de proceso válida.

Cada validación registra:

```text
_run_id
_batch_id
tenant_id
layer
table_name
check_name
check_severity
records_checked
records_failed
check_passed
executed_at
```

Los resultados se almacenan en:

```text
data/shared/quality_logs
```

Si:

```yaml
quality:
  fail_on_critical: true
```

y falla una validación crítica, el procesamiento del tenant se detiene antes de Gold.

### Gold

Se genera la tabla:

```text
daily_metrics_by_delivery_type
```

con granularidad:

```text
tenant_id
fecha_proceso
tipo_entrega
```

y las métricas:

```text
total_units
total_revenue
active_routes
active_transports
```

`total_revenue` utiliza el precio real de la transacción:

```text
cantidad_normalizada_st * precio
```

El campo `precio_base` del catálogo se conserva únicamente como información descriptiva.

## Estructura del repositorio

```text
saas-data-platform/
├── README.md
├── pyproject.toml
├── .github/
│   └── workflows/
│       └── ci.yml
├── config/
│   ├── base.yaml
│   ├── env/
│   │   ├── dev.yaml
│   │   ├── qa.yaml
│   │   └── main.yaml
│   └── tenants/
│       ├── sv.yaml
│       ├── hn.yaml
│       ├── gt.yaml
│       ├── pe.yaml
│       ├── ec.yaml
│       └── jm.yaml
├── docs/
│   ├── infra.md
│   ├── observations.md
│   └── onboarding-tenant.md
├── mentoring/
│   ├── bad_code.py
│   ├── good_code.py
│   └── code_review.md
├── src/
│   └── saas_pipeline/
│       ├── bronze.py
│       ├── silver.py
│       ├── quality.py
│       ├── gold.py
│       ├── config.py
│       └── cli.py
└── tests/
    ├── test_quality.py
    └── test_silver_transforms.py
```

## Configuración

La configuración utiliza la siguiente jerarquía:

```text
base.yaml
   <
env/<environment>.yaml
   <
tenants/<tenant>.yaml
   <
parámetros CLI
```

Ejemplo:

```text
config/base.yaml
config/env/dev.yaml
config/tenants/sv.yaml
```

Esto permite reutilizar el mismo pipeline entre diferentes ambientes y tenants.

## Instalación

Crear un entorno virtual:

```bash
python -m venv .venv
```

En Git Bash:

```bash
source .venv/Scripts/activate
```

Instalar dependencias:

```bash
python -m pip install --upgrade pip
pip install pyspark==3.5.4
pip install delta-spark==3.2.0
pip install omegaconf==2.3.1
pip install pytest==9.1.1
pip install ruff==0.16.4
```

También se requiere Java 11.

## Archivos de entrada

Colocar los archivos en:

```text
data/raw/global_mobility_data_entrega_productos.csv
data/raw/materials_catalog.csv
```

El directorio `data/` no se versiona en Git.

## Ejecutar un tenant

Ejemplo para SV:

```bash
python -m src.saas_pipeline.cli \
  --env dev \
  --tenant sv \
  --start-date 2025-01-01 \
  --end-date 2025-03-31
```

## Ejecutar todos los tenants

```bash
python -m src.saas_pipeline.cli \
  --env dev \
  --tenant all \
  --start-date 2025-01-01 \
  --end-date 2025-03-31
```

La opción `execution.fail_fast` determina si una ejecución multi-tenant debe detenerse ante el primer error o continuar con los demás tenants.

## Idempotencia

La estrategia depende de cada capa.

### Bronze

Sobrescribe las particiones correspondientes al rango solicitado.

### Silver

Utiliza Delta Lake `MERGE INTO` mediante la llave de negocio:

```text
tenant_id
fecha_proceso
transporte
ruta
material
tipo_entrega
```

### Gold

Gold se considera una capa derivada y recalcula las particiones correspondientes al rango ejecutado.

## Tests

Ejecutar:

```bash
pytest -v
```

Las pruebas cubren reglas como:

- conversión de CS a ST;
- filtrado de tipos de entrega;
- detección de cantidades inválidas;
- flags de tipo de entrega;
- carga de configuración YAML.

## Linter

Ejecutar:

```bash
ruff check .
```

## CI/CD

El repositorio incluye GitHub Actions.

El workflow se ejecuta automáticamente en:

```text
push
pull_request
```

y ejecuta:

```text
Ruff
Pytest
```

## Onboarding de un nuevo tenant

El procedimiento se encuentra documentado en:

```text
docs/onboarding-tenant.md
```

El diseño busca que agregar un nuevo tenant requiera principalmente configuración y no duplicación del pipeline.

## Infraestructura

`docs/infra.md` describe una propuesta de infraestructura Azure utilizando:

- Azure Databricks;
- ADLS Gen2;
- Unity Catalog;
- Databricks Workflows;
- Azure Key Vault;
- Terraform.

El snippet de Terraform es ilustrativo y no requiere conectarse a una suscripción Azure real.

## Observaciones de arquitectura

Las decisiones, ambigüedades y posibles mejoras se encuentran documentadas en:

```text
docs/observations.md
```

## Mentoría

La carpeta `mentoring/` contiene el ejercicio de revisión de código solicitado.

```text
bad_code.py
good_code.py
code_review.md
```

`bad_code.py` conserva el código original del Anexo A.

`code_review.md` documenta las observaciones realizadas como Senior Data Engineer.

`good_code.py` contiene una versión refactorizada utilizando transformaciones nativas de Spark.

## Qué dejé fuera y por qué

No se implementó infraestructura Azure real ni un módulo Terraform ejecutable porque la prueba solicita únicamente documentación y un snippet ilustrativo de infraestructura.

No se implementaron Azure Data Factory ni Databricks Workflows funcionales. Para la prueba local, el CLI actúa como punto de entrada y orquestador. En un ambiente productivo, Databricks Workflows podría invocar este mismo entry point utilizando parámetros de tenant y rango de fechas.

Tampoco se implementaron los bonus opcionales, como streaming, Auto Loader, una segunda tabla Gold o dashboards. Se priorizó el alcance obligatorio: funcionalidad del pipeline, idempotencia, multi-tenancy, SCD Type 2, calidad de datos, CI/CD, pruebas y documentación.