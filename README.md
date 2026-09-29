# Proyecto Big Data — eCommerce

Proyecto final de Electiva Profesional IV (Big Data), Ingeniería de Sistemas. El caso analiza el recorrido de visitas a productos y compras de una tienda online multicategoría.

## Inicio rápido para el equipo

Estos pasos son para clonar el proyecto en otro computador y dejarlo listo para ejecutar. Los datos originales no están en Git por tamaño; cada integrante debe ubicarlos localmente.

Requisitos probados:

- Python 3.11.
- Java instalado y disponible para Spark; el proyecto fue probado con Java 23 en Windows.
- PowerShell en Windows.
- Los archivos `2019-Oct.csv` y `2019-Nov.csv` en `data/raw/`.

Desde PowerShell:

```powershell
git clone https://github.com/marlonstv120/ecommerce-big-data.git
cd ecommerce-big-data

python -m venv .venv
& ".\.venv\Scripts\Activate.ps1"
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m ipykernel install --prefix .venv --name ecommerce-big-data --display-name "Python 3.11 (eCommerce Big Data)"
```

Después crea la carpeta de datos y copia allí los CSV descargados de Kaggle:

```powershell
New-Item -ItemType Directory -Force "data\raw"
```

La estructura esperada es:

```text
data/raw/2019-Oct.csv
data/raw/2019-Nov.csv
```

Para verificar que el entorno quedó bien:

```powershell
python -m unittest discover -s tests -v
python -m pip check
```

`data/raw/`, `data/processed/`, `data/demo/` y `.venv/` están ignorados por Git. No se deben subir los CSV, Parquet generados, modelo entrenado ni el entorno virtual.

## Dataset y alcance local

- **Dataset:** `eCommerce behavior data from multi category store`.
- **Fuente:** REES46 / [Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).
- **Periodo publicado:** octubre de 2019 a abril de 2020.
- **Archivos presentes:** `data/raw/2019-Oct.csv` y `data/raw/2019-Nov.csv`; `archive.zip` contiene los dos CSV comprimidos.
- **Cobertura de este análisis:** octubre y noviembre de 2019; dos de los siete meses del periodo publicado.
- **Evidencia calculada con PySpark:** 109.950.743 filas y 14.675.375.250 bytes (13,668 GiB) sin comprimir. No se cuenta el ZIP de nuevo.

Cada fila representa un evento. Los eventos observados en los archivos locales son `view`, `cart` y `purchase`; `remove_from_cart` no aparece en estos dos meses.

El análisis y sus límites están en [`docs/analisis_exploratorio_fase1.md`](docs/analisis_exploratorio_fase1.md); las decisiones de arquitectura y el diagrama de seis capas están en [`docs/arquitectura_fase1.md`](docs/arquitectura_fase1.md).

## Fase 1: reproducir el análisis

El entorno de Python se crea localmente en `.venv/`. Las versiones directas instaladas y probadas están registradas en [`requirements.txt`](requirements.txt). No se usa Pandas para leer los CSV.

En PowerShell, desde la raíz del proyecto:

```powershell
& ".\.venv\Scripts\Activate.ps1"
python -m pip install -r requirements.txt
python -m ipykernel install --prefix .venv --name ecommerce-big-data --display-name "Python 3.11 (eCommerce Big Data)"
python src\eda.py --input data\raw\2019-Oct.csv data\raw\2019-Nov.csv --output data\output\eda_summary.json
```

El script lee ambos archivos con PySpark local, conserva el detalle completo en agregaciones compactas y escribe `data/output/eda_summary.json`. Usa dos trabajadores locales, persistencia temporal a disco y configuración JVM compatible con el Java 23 probado en Windows. No sobrescribe los originales.

Abre [`notebooks/01_exploracion.ipynb`](notebooks/01_exploracion.ipynb) desde la raíz del proyecto y selecciona el kernel **Python 3.11 (eCommerce Big Data)**. Por defecto, el notebook lee el resumen existente y genera gráficos sin volver a escanear los archivos. Para recalcular todo, ejecuta el comando `src\eda.py` anterior o cambia `RUN_FULL_SCAN = True` en el cuaderno.

Pruebas de la lógica de calidad y funnel con archivos pequeños:

```powershell
python -m unittest discover -s tests -v
```

## Fase 2: pipeline Batch local

El MVP de PySpark implementa Bronze → Silver → Gold con los dos meses locales. Los CSV de `data/raw/` son Bronze inmutable; Silver se escribe en Parquet tipado/deduplicado y Gold contiene métricas de funnel, categoría y tiempo. La deduplicación elimina en Silver 130.739 filas excedentes detectadas por huella SHA-256; el resumen registra calidad, filas rechazadas y conteos de salida.

```powershell
python src\run_pipeline.py `
  --input data\raw\2019-Oct.csv data\raw\2019-Nov.csv `
  --silver-output data\processed\silver\events `
  --gold-output data\processed\gold `
  --summary-output data\output\pipeline_summary.json `
  --shuffle-partitions 128
```

El pipeline usa el adaptador Java local de `lib/` para que Spark pueda escribir Parquet en NTFS sin instalar `winutils.exe` ni servicios Hadoop. La dependencia está descrita en [`lib/README.md`](lib/README.md). Los outputs Parquet en `data/processed/` se regeneran al ejecutar el comando; `.gitignore` los mantiene fuera del control de versiones.

El resumen de la última ejecución está en `data/output/pipeline_summary.json`. El detalle de decisiones y comandos, incluidas las incidencias técnicas y su resolución, está en [`docs/bitacora_proceso.md`](docs/bitacora_proceso.md).

## Fase 3: modelo y dashboard local

El clasificador de referencia usa Spark MLlib para estimar si una sesión comprará **después** de sus primeros cinco minutos. Usa solo actividad anterior al corte, entrena con octubre, prueba temporalmente con noviembre, muestrea de forma determinista el 2 % de sesiones y aplica pesos de clase. Las métricas incluyen recall, precisión, F1, ROC-AUC y PR-AUC; consulta `data/output/model_evaluation.json` y sus limitaciones antes de interpretar las predicciones.

Entrenar o volver a evaluar el modelo:

```powershell
python src\train_model.py `
  --silver-input data\processed\silver\events `
  --eda-summary data\output\eda_summary.json `
  --model-output data\processed\models\session_purchase_after_5m `
  --metrics-output data\output\model_evaluation.json `
  --sample-percent 2 `
  --cutoff-minutes 5
```

Abrir el dashboard:

```powershell
python -m streamlit run dashboard\app.py
```

El dashboard lee solo Gold y las métricas compactas del modelo; ofrece filtros de mes, fecha y categoría, KPIs, funnel, compras por periodo/categoría y visualizaciones de evaluación del modelo. Streamlit instala Pandas indirectamente, pero la app no usa Pandas para cargar eventos ni lee los CSV raw.

La presentación ejecutiva está en `docs/big_data_poyecto_final.pptx`. No hay dashboard alojado en cloud ni servicio continuo; todo corre localmente.

## Arquitectura recomendada

El diseño usa un Data Lake local con organización Medallion, ELT y Batch. La comparación de Warehouse/Lake/Lakehouse y el diagrama de seis capas están en [`docs/arquitectura_fase1.md`](docs/arquitectura_fase1.md). Streamlit se implementó como dashboard local de la Fase 3 y la presentación ejecutiva quedó incluida como archivo PowerPoint.

**Nota de estructura:** el proyecto ya tenía una carpeta vacía llamada `data/proccesed/` (con esa ortografía). No se renombró ni se usó; la ruta recomendada para futuras capas es `data/processed/`, actualmente protegida por `.gitignore`.

## Datos y entorno local

Los CSV originales y el entorno `.venv/` se mantienen locales. `.gitignore` excluye `data/raw/`, `data/demo/`, `data/processed/` y `.venv/`; no se deben subir los datasets, muestras locales, Parquet, modelos entrenados ni el entorno virtual al repositorio. Los resultados resumidos de `data/output/` no contienen filas de eventos.

El repositorio remoto público está en <https://github.com/marlonstv120/ecommerce-big-data>. Antes de preparar nuevos commits, revisar `git status --short` y añadir únicamente código, documentación, presentación, evidencias ligeras y resultados resumidos.
