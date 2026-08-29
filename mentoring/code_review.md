# Code Review

## 1. Uso de Pandas para procesamiento de datos

### Qué está mal

El código utiliza `pandas.read_csv()` para cargar el dataset y posteriormente convierte el resultado a un DataFrame de Spark.

### Por qué importa

Esto obliga a cargar los datos en memoria en una sola máquina y elimina las ventajas del procesamiento distribuido de Spark. Con volúmenes grandes puede provocar problemas de memoria y rendimiento.

### Cómo lo corregiría

Leer el archivo directamente con Spark:

```python
df = spark.read.option("header", True).csv(file_path)
```

De esta manera el procesamiento puede distribuirse entre los executors.

## 2. Iteración fila por fila

### Qué está mal

Se utiliza `df.iterrows()` y lógica Python para procesar individualmente cada registro.

### Por qué importa

El procesamiento fila por fila es costoso y no aprovecha las optimizaciones del motor de Spark.

### Cómo lo corregiría

Utilizar transformaciones declarativas de Spark como `filter`, `withColumn`, `when` y `otherwise`.

Por ejemplo:

```python
F.when(
    F.upper(F.col("unidad")) == "CS",
    F.col("cantidad") * 20
).otherwise(F.col("cantidad"))
```

## 3. Reglas de negocio incompletas y hardcoded

### Qué está mal

El código solamente considera `ZPRE` y `ZVE1`, y las reglas se encuentran directamente dentro de la función.

### Por qué importa

La solución se vuelve difícil de mantener y además ignora otros tipos válidos del negocio como `Z04` y `Z05`.

### Cómo lo corregiría

Definir explícitamente los tipos permitidos:

```python
VALID_DELIVERY_TYPES = ["ZPRE", "ZVE1", "Z04", "Z05"]
```

La configuración dependiente del ambiente o tenant debería mantenerse separada de la lógica de transformación.

## 4. Ausencia de validaciones de calidad

### Qué está mal

El código no valida fechas inválidas, cantidades nulas o no positivas, precios nulos ni materiales desconocidos.

### Por qué importa

Los registros incorrectos podrían llegar directamente a las capas analíticas y producir métricas erróneas.

### Cómo lo corregiría

Implementar validaciones antes de persistir Silver y separar los registros inválidos en quarantine utilizando una razón como `_quarantine_reason`.

Los resultados de las validaciones también deberían persistirse para permitir observabilidad.

## 5. Escritura no idempotente

### Qué está mal

El código utiliza:

```python
sdf.write.mode("overwrite").parquet("/tmp/output/" + country)
```

Esto sobrescribe completamente el resultado del país y no considera particiones ni una llave de negocio.

### Por qué importa

Un reproceso parcial puede eliminar información previamente procesada.

### Cómo lo corregiría

Utilizar Delta Lake y una estrategia de idempotencia apropiada. Para una tabla Silver utilizaría `MERGE INTO` mediante una llave de negocio.

## 6. Falta de diseño multi-tenant

### Qué está mal

El país se recibe como parámetro, pero la solución solamente filtra el DataFrame y construye una ruta manualmente.

### Por qué importa

Un filtro por país no constituye por sí solo una arquitectura multi-tenant y aumenta el riesgo de mezclar información entre tenants.

### Cómo lo corregiría

Utilizar configuración jerárquica por ambiente y tenant, junto con rutas o schemas independientes para cada tenant.

## 7. Ausencia de pruebas

### Qué está mal

Las reglas de transformación no tienen pruebas automatizadas.

### Por qué importa

Cambios futuros podrían modificar inadvertidamente reglas importantes como la conversión de unidades o los tipos de entrega permitidos.

### Cómo lo corregiría

Separaría las transformaciones en funciones testeables y agregaría pruebas unitarias con `pytest`.

# Cómo se lo explicaría al junior

Le explicaría que el objetivo del code review no es únicamente señalar errores, sino ayudar a que la solución pueda trabajar con volúmenes mayores, sea mantenible y pueda operarse de forma segura.

Comenzaría reconociendo que la implementación permite entender la regla básica de negocio y produce un resultado, pero le mostraría la diferencia entre una solución que funciona con un dataset pequeño y una solución preparada para una plataforma de datos basada en Spark.

Revisaría especialmente el uso de transformaciones nativas de Spark en lugar de Pandas e iteraciones fila por fila, la separación entre configuración y reglas de negocio, el manejo explícito de datos inválidos y el concepto de idempotencia.

Como temas para investigar por su cuenta le pediría revisar Spark DataFrames y transformaciones declarativas, lazy evaluation, diferencias entre Pandas y Spark, particionamiento, Delta Lake `MERGE`, estrategias de idempotencia y patrones básicos de Data Quality.

En una siguiente revisión le pediría aplicar estos conceptos a una transformación pequeña y agregar pruebas unitarias antes de ampliar la solución.