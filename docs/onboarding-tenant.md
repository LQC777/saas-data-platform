# Onboarding de un nuevo tenant

La solución fue diseñada para agregar nuevos tenants principalmente mediante configuración, evitando duplicar pipelines.

## 1. Crear configuración del tenant

Crear un nuevo archivo:

`config/tenants/<tenant>.yaml`

Por ejemplo, si se incorpora Costa Rica con el código `cr`, se crearía:

`config/tenants/cr.yaml`

con el siguiente contenido:

```yaml
tenant:
  code: cr
```

## 2. Registrar el tenant

Agregar el código del nuevo tenant a la lista `TENANTS` definida en `src/saas_pipeline/cli.py`.

Por ejemplo:

```python
TENANTS = [
    "sv",
    "hn",
    "gt",
    "pe",
    "ec",
    "jm",
    "cr",
]
```

Esto permite que el nuevo tenant pueda ejecutarse individualmente y también forme parte de una ejecución con `--tenant all`.

No es necesario modificar la lógica de Bronze, Silver, Quality o Gold.

## 3. Datos de origen

Los registros del nuevo tenant deben estar disponibles en el dataset RAW e identificarse mediante el campo `pais`.

Durante la ejecución, Bronze selecciona únicamente los registros correspondientes al tenant solicitado.

## 4. Aislamiento de datos

Cada tenant mantiene rutas independientes.

Para el ejemplo `cr` se generarían rutas como:

```text
data/bronze/cr/
data/silver/cr/
data/gold/cr/
data/silver_quarantine/cr/
```

De esta manera, el aislamiento no depende únicamente de un filtro lógico por tenant.

En una implementación productiva con Databricks y Unity Catalog, este aislamiento podría representarse mediante schemas y permisos específicos por tenant.

## 5. Configuración por ambiente

La configuración común se mantiene en:

`config/base.yaml`

y las diferencias por ambiente se encuentran en:

```text
config/env/dev.yaml
config/env/qa.yaml
config/env/main.yaml
```

Por lo tanto, incorporar un tenant no requiere duplicar la configuración completa del pipeline.

## 6. Validar el nuevo tenant

Antes de procesar un rango completo, se recomienda realizar una ejecución controlada.

Ejemplo:

```bash
python -m src.saas_pipeline.cli \
  --env dev \
  --tenant cr \
  --start-date 2025-01-01 \
  --end-date 2025-01-31
```

Se deben revisar los resultados de:

- Bronze
- Silver
- Quarantine
- Data Quality
- Gold

También se debe comprobar que no existan datos de otros tenants dentro de las rutas correspondientes al nuevo tenant.

## 7. Ejecución multi-tenant

Una vez validado el nuevo tenant, podrá incluirse en la ejecución general:

```bash
python -m src.saas_pipeline.cli \
  --env dev \
  --tenant all \
  --start-date 2025-01-01 \
  --end-date 2025-03-31
```

El mismo código del pipeline se reutiliza para todos los tenants; las diferencias se administran principalmente mediante configuración.