# Bitácora del proceso — eCommerce Big Data

Este documento registra qué se hizo, por qué se tomó cada decisión y cómo reproducirlo. Está redactado para que el equipo pueda explicar el trabajo con sus palabras: Spark ejecutó los escaneos y agregaciones; el equipo definió las preguntas, reglas y decisiones. No se atribuyen cálculos manuales que hizo el pipeline.

## Contexto y alcance actual

- Dataset: `eCommerce behavior data from multi category store`, REES46/Kaggle.
- Archivos disponibles y procesados: octubre y noviembre de 2019. No se descargaron meses adicionales.
- El curso exige Spark sobre conjuntos mayores a 10 millones de filas. Octubre tiene 42.448.764 y noviembre 67.501.979; juntos, 109.950.743 antes de deduplicar.
- La ruta solicitada inicialmente, `docs/proyecto_final_big_data.md`, no existe; se trabajó con la guía académica local disponible durante el desarrollo. Esa guía no se versiona en el repositorio porque no es producción propia del equipo.
- No se usan Pandas para leer los CSV. No estaba instalado al comenzar Fase 1; Streamlit lo incorporó después como dependencia indirecta, pero la app usa Spark y tablas Gold pequeñas, no los eventos en Pandas. Los datos originales siguen bajo `data/raw/` y están ignorados por Git.

## Fase 1 — Exploración y diseño arquitectónico (completada)

### Pasos realizados

1. Se leyó la guía y se extrajeron sus requisitos de 5V, comparación de almacenamiento, procesamiento ETL/ELT y Batch/Streaming/Lambda/Kappa, problema con tres preguntas y diagrama de seis capas.
2. Se inspeccionó el disco: están `2019-Oct.csv`, `2019-Nov.csv` y `archive.zip`. El ZIP contiene entradas de esos dos meses, no otros meses que se deban sumar al análisis.
3. Se verificó que `.venv` funcionaba con Python 3.11.9 y pip 24.0. Antes de instalar se comprobó que no había Pandas, PySpark ni Jupyter.
4. Se instalaron PySpark 4.0.1, JupyterLab 4.6.4, ipykernel 7.3.0 y Matplotlib 3.11.2 en el `.venv` existente. En Fase 1 no se instaló ni usó Pandas.
5. Se probó Spark local antes de leer los CSV. Java 23 requiere `-Djava.security.manager=allow` para la llamada de Hadoop UserGroupInformation. El escaneo se ajustó a `local[2]`, heap de 1 GB, `SerialGC` y persistencia temporal en disco para adaptarse al límite de memoria comprometida de Windows.
6. Se escribieron pruebas con CSV pequeños para nulos, timestamps, tipos de evento, valores numéricos, duplicados y orden temporal del funnel.
7. `src/eda.py` leyó los dos CSV completos con un esquema explícito, validó las conversiones, calculó calidad/frecuencia/métricas y produjo `data/output/eda_summary.json`.
8. Se compararon las huellas SHA-256 de ambos CSV antes y después: son idénticas.
9. Se documentaron las 5V, las preguntas de negocio, el Data Lake local, ELT, Batch y las seis capas. El detalle está en `docs/analisis_exploratorio_fase1.md` y `docs/arquitectura_fase1.md`.
10. Se creó `notebooks/01_exploracion.ipynb`, se registró el kernel en `.venv` y se ejecutó con nbconvert. El notebook muestra agregados y cuatro gráficos; no vuelve a escanear el CSV por defecto.

### Problemas encontrados y resolución

- **Java 23 y Spark:** el primer smoke test falló en `Subject.getSubject`. Se resolvió con opciones de JVM locales a la sesión (`-Djava.security.manager=allow`) y una configuración de bajo consumo; no se instaló otro JDK.
- **Memoria de Windows:** una ejecución con heap de 4 GB y `local[4]` falló al reservar memoria nativa. La configuración `local[2]`, heap de 1 GB y `SerialGC` permitió completar el EDA y el pipeline.
- **Tiempo del primer diseño EDA:** analizar con detalle octubre, noviembre y luego el consolidado repetía agregaciones costosas y excedió el tiempo disponible. Se cambió a detalle completo una vez sobre el consolidado y un resumen de volumen/calidad/eventos por mes. Así se obtuvo el resultado completo sin omitir meses.
- **Parquet en Windows:** leer CSV funcionaba, pero la escritura Spark fallaba porque Hadoop intentaba ejecutar `winutils.exe` para permisos POSIX. Se añadió el pequeño adaptador local Apache-2.0 `lib/hadoop-bare-naked-local-fs-0.1.0.jar`, que usa la API Java para el filesystem local. El script verifica su SHA-256 y lo coloca en el JAR directory del `.venv` antes de arrancar Spark. No se instala ni ejecuta un servicio Hadoop. El motivo y la licencia están en `lib/README.md`.
- **Python worker en Windows:** al probar el feature builder del modelo, Spark intentó arrancar el alias de Python de Microsoft Store y no lo encontró. `build_spark_session` ahora fija `PYSPARK_PYTHON` en el intérprete activo de `.venv`; la prueba de features del modelo pasó.

### Resultados que defendemos

- 109.950.743 filas en 13,668 GiB sin comprimir, en dos meses.
- `category_code`: 35.413.780 nulos; `brand`: 15.331.243; `user_session`: 12.
- No se detectaron fechas inválidas, registros corruptos, conversiones numéricas inválidas ni eventos fuera del dominio esperado.
- El método de huella SHA-256 detectó 130.739 filas excedentes duplicadas en 75.652 grupos. La fuente no se modificó.
- Los resultados de precio, funnel y categorías son observacionales. La suma de `price` en `purchase` es valor estimado por evento, no ingresos netos ni margen.

Los números completos y sus limitaciones están en `docs/analisis_exploratorio_fase1.md`; las preguntas, decisiones y arquitectura en `docs/arquitectura_fase1.md`.

## Fase 2 — Implementación técnica (pipeline MVP completado)

### Modelo de procesamiento

1. **Bronze:** los CSV originales en `data/raw/` se usan como fuente inmutable; no se duplica el dataset.
2. **Silver:** `src/pipeline.py` lee con PySpark, convierte timestamps a UTC e IDs/precio a tipos numéricos, conserva nulos permitidos y añade `event_month` y `price_quality`. Los registros corruptos, fechas/tipos esenciales inválidos y conversiones no válidas se cuentan como rechazados; en los dos archivos observados fueron 0.
3. **Deduplicación:** en Silver se eliminan filas con la misma huella SHA-256 canónica. En estos CSV se removieron 130.739 excedentes; Bronze permanece intacto. Una repetición exacta puede representar una retransmisión o dos eventos idénticos ocurridos en el mismo segundo; la política y esa limitación se informan.
4. **Precios:** se conservan en Silver; cero/no positivo queda marcado como `price_quality=nonpositive`. Los candidatos IQR no se recortan automáticamente.
5. **Gold:** se generan tablas Parquet de funnel mensual, funnel por día-de-semana/hora UTC, compras por categoría y compras por fecha/hora UTC.

### Ejecución reproducible

Desde la raíz, con `.venv` activo:

```powershell
python src/run_pipeline.py `
  --input data/raw/2019-Oct.csv data/raw/2019-Nov.csv `
  --silver-output data/processed/silver/events `
  --gold-output data/processed/gold `
  --summary-output data/output/pipeline_summary.json `
  --shuffle-partitions 128
```

El comando sobrescribe solamente sus salidas procesadas para que el pipeline sea repetible. No escribe en `data/raw/`.

### Resultado actual del pipeline

- Entradas Bronze: 109.950.743 filas; filas rechazadas: 0.
- Silver Parquet: 109.820.004 filas, particionadas por `event_month`.
- Duplicados exactos removidos de Silver: 130.739 (método SHA-256); los CSV Bronze se mantienen iguales.
- Gold: 2 filas de funnel mensual, 336 combinaciones de funnel día-de-semana/hora UTC, 128 categorías, 6.429 combinaciones categoría/día y 1.415 agrupaciones de compras fecha/hora.
- Faltantes de categoría/marca y 256.761 precios no positivos se conservan/identifican y quedan registrados en el resumen de calidad.
- Resumen: `data/output/pipeline_summary.json`.

Ejemplos de Gold después de deduplicar: la conversión vista-compra por sesión fue 3,13 % en octubre y 4,67 % en noviembre; `electronics.smartphone` tuvo 720.625 eventos `purchase` y una suma de `price` de 334.855.345,68. La mayor cantidad de eventos de compra ocurrió a las 09:00 UTC (126.616). Son métricas sobre eventos observados, no ingresos netos ni conclusiones causales.

Los rechazos fueron cero, por lo que en esta ejecución no se creó una zona de cuarentena; si futuras entradas contienen filas inválidas, el resumen conserva los conteos de rechazo y Bronze sigue siendo la fuente para corregir/reprocesar.

La configuración usa dos trabajadores locales, heap de 1 GB y 128 particiones de shuffle. Los 128 partitions afectan shuffle; no se reserva memoria equivalente a 128 trabajadores.

### Pruebas ejecutadas

```powershell
python -m unittest discover -s tests -v
python -m pip check
```

La suite cubre tanto EDA como transformación de tipos, deduplicación, nulos permitidos, transiciones cronológicas del funnel, escritura/parsing Parquet, outputs Gold sobre fixtures pequeños y construcción de features sin fuga temporal. La suite completa pasó con 10 pruebas.

## Repositorio y protección de datos

El repositorio quedó publicado en GitHub en la rama `main`. `.gitignore` excluye `.venv/`, `data/raw/`, `data/demo/`, `data/processed/`, cachés y logs JVM. Los archivos de código, documentación, notebook, resumen pequeño, presentación, evidencia ligera y adaptador local quedan visibles para control de versiones. Antes de publicar nuevos cambios, se debe revisar `git status` y confirmar que ningún CSV, Parquet, modelo generado o entorno virtual se añada.

## Fase 3 — Modelo, dashboard y presentación completados

### Modelo ML

- Se usó una regresión logística ponderada de **Spark MLlib**, apropiada para el stack Spark del proyecto. El problema se definió como: con la actividad observada durante los primeros cinco minutos, estimar si ocurre una compra en o después de ese corte.
- Para evitar fuga de información, las features se calculan solo con eventos no `purchase` anteriores al corte. Se excluyen las sesiones que compraron antes de los cinco minutos y las sesiones censuradas por los límites temporales disponibles.
- Se muestreó determinísticamente el 2 % de las sesiones usando una huella de `user_session`; octubre se usó para entrenar y noviembre como prueba temporal. No se usó `user_id` ni identificadores de producto como features. El desbalance del target se manejó con pesos de clase calculados en el entrenamiento. El umbral de decisión quedó en 0,5 y no se ajustó mirando el test.
- Muestra: 177.051 sesiones de entrenamiento, con 4.043 positivas; 265.194 sesiones de test, con 5.411 positivas.
- Test de noviembre: **ROC-AUC 0,7961**, **PR-AUC 0,0965**, recall **73,89 %**, precisión **4,96 %** y F1 **0,0929**. La matriz tuvo 3.998 verdaderos positivos, 76.619 falsos positivos, 183.164 verdaderos negativos y 1.413 falsos negativos.
- Interpretación defendible: el modelo ordena sesiones mejor que azar y, al umbral 0,5 con pesos de clase, detecta cerca de tres cuartas partes de las positivas, pero genera muchos falsos positivos. No se recomienda usarlo como decisión automática; el umbral depende del costo de intervención y debe validarse antes de cualquier uso operacional.
- Artefactos: modelo en `data/processed/models/session_purchase_after_5m/`; métricas, matriz y deciles en `data/output/model_evaluation.json`.

### Dashboard

- Se instalaron Streamlit 1.64.0 y Plotly 7.1.0. Streamlit incorpora Pandas 3.0.6 y PyArrow 25.0.1 como dependencias indirectas; el dashboard no lee los CSV con Pandas. Consulta solamente las tablas Gold pequeñas mediante Spark y representa los datos con Plotly.
- `dashboard/app.py` presenta KPIs y gráficos de funnel mensual, compras por fecha, compras por categoría, conversión por día/hora UTC, volumen de compras por hora UTC, matriz de confusión y tasa observada por decil de probabilidad.
- Los filtros incluyen mes, intervalo de fechas y categoría. La tasa de valor estimado se identifica como suma de `price` por evento, no ingresos netos ni margen.
- El smoke test de Streamlit encontró 0 excepciones, 9 métricas y 7 gráficos Plotly.
- Inicio local: `python -m streamlit run dashboard/app.py`.

La presentación ejecutiva quedó en `docs/big_data_poyecto_final.pptx`. Antes de presentarla, se deben ensayar la demo de Streamlit y la explicación de la definición del target, el muestreo, la división temporal y la baja precisión del modelo; no se debe resumir solo el ROC-AUC ni describir el modelo como causal.
