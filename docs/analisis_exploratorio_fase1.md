# Análisis exploratorio — REES46 eCommerce Behavior

## Alcance y procedencia

Se analizaron con Spark los dos CSV disponibles localmente: octubre y noviembre de 2019. El archivo `data/raw/archive.zip` contiene copias comprimidas de esos mismos dos meses; no se contó dos veces. Los CSV originales se conservaron sin modificaciones.

La guía del curso está en `docs/proyecto_final_big_data_2.md`. Su estimación publicada para el dataset completo es aproximadamente 7,5 GB y más de 285 millones de eventos entre octubre de 2019 y abril de 2020. Esa cifra es contexto publicado, no el tamaño ni el conteo calculado de los archivos locales.

### Método reproducible

- Entorno: Python 3.11.9, PySpark 4.0.1, JupyterLab 4.6.4, ipykernel 7.3.0 y Matplotlib 3.11.2.
- Se utilizó PySpark en modo local (`local[2]`), con persistencia a disco y heap de 1 GB; los CSV se leen como cadenas con un esquema explícito y se validan las conversiones a tipos semánticos.
- Se procesaron octubre y noviembre en una lectura consolidada. El detalle se agrega una vez para el consolidado; los conteos, periodos, dominios de eventos y faltantes se presentan también por archivo.
- El notebook lee únicamente `data/output/eda_summary.json`. No carga los CSV en Pandas.
- La JVM usa `-Djava.security.manager=allow` para compatibilidad del Hadoop UserGroupInformation de Spark 4.0.1 con el Java 23 instalado. El script configura esta opción localmente al crear Spark.
- Comando para reproducir el escaneo completo:

```powershell
& ".venv\Scripts\python.exe" src\eda.py --input data\raw\2019-Oct.csv data\raw\2019-Nov.csv --output data\output\eda_summary.json
```

Las cifras calculadas a continuación están respaldadas por `data/output/eda_summary.json`. Los tamaños, conteos y tasas se calcularon sobre los archivos locales con Spark; los valores de la guía se identifican por separado como publicados.

## Inventario local y volumen

| Archivo | Bytes | Tamaño binario | Filas validadas por Spark | Periodo UTC |
|---|---:|---:|---:|---|
| `2019-Oct.csv` | 5.668.612.855 | 5,279 GiB | 42.448.764 | 2019-10-01 00:00:00 a 2019-10-31 23:59:59 |
| `2019-Nov.csv` | 9.006.762.395 | 8,388 GiB | 67.501.979 | 2019-11-01 00:00:00 a 2019-11-30 23:59:59 |
| **Consolidado** | **14.675.375.250** | **13,668 GiB** | **109.950.743** | **2019-10-01 a 2019-11-30** |

`archive.zip` ocupa 4.606.720.907 bytes (4,290 GiB) y contiene los dos CSV comprimidos; no se suma al tamaño sin comprimir del conjunto. Los dos meses locales cubren 2 de los 7 meses descritos en la guía (28,6 % del periodo calendario), pero no se infiere de ello que representen la misma fracción de los eventos del dataset completo.

El umbral de la guía para usar Spark se supera ampliamente: cada archivo mensual por separado tiene más de 10 millones de filas. No afirmamos que 13,668 GiB excedan la capacidad de toda base relacional; una base bien dimensionada podría almacenar este subconjunto. Spark se justifica aquí por el umbral obligatorio del curso, por la escala publicada del conjunto completo y porque permite ejecutar las agregaciones sobre los 109,9 millones de eventos sin cargar el CSV en memoria como un único DataFrame de Pandas.

## Tipos, columnas y variedad

Los archivos son CSV delimitados con una cabecera fija de nueve campos. En la lectura de origen Spark conserva las columnas como cadenas para no inferir tipos mediante otra pasada; la validación semántica encontró estas conversiones válidas:

| Columna | Tipo semántico validado | Uso en el análisis |
|---|---|---|
| `event_time` | timestamp UTC | periodo, frecuencia y orden de eventos |
| `event_type` | categoría textual | `view`, `cart`, `purchase` y dominio permitido |
| `product_id` | entero de 64 bits | identificación del producto |
| `category_id` | entero de 64 bits | identificación de la categoría |
| `category_code` | texto jerárquico nullable | agrupación por categoría |
| `brand` | texto nullable | atributo de producto |
| `price` | numérico nullable | cuantiles y suma aproximada en eventos de compra |
| `user_id` | entero de 64 bits | usuario |
| `user_session` | identificador textual nullable | funnel por sesión |

La guía clasifica el caso como semi-estructurado. El material local, en cambio, tiene estructura tabular fija de nueve columnas; `category_code` contiene nombres jerárquicos separados por puntos. Los formatos realmente presentes son CSV y un ZIP que contiene los mismos CSV. No se integró una fuente adicional.

## Evidencia para las 5V

| V | Evidencia calculada o publicada | Interpretación y decisión |
|---|---|---|
| **Volume** | 109.950.743 filas; 14.675.375.250 bytes (13,668 GiB) sin comprimir en dos meses. Ambos archivos superan individualmente 10 millones de filas. | Analizar con PySpark local según la regla del curso. La cifra de 7,5 GB / 285+ millones de eventos corresponde a la estimación publicada para el periodo completo, no a estos archivos. |
| **Velocity** | Eventos con timestamp válido en 61 días y 1.464 horas UTC observadas. Promedio de 75.103 eventos por hora activa; máximo de 480.916 en la hora iniciada `2019-11-17 14:00 UTC`. | Son conteos agrupados por `event_time`, no velocidad de llegada de un sistema vivo. Los insumos son archivos históricos: se recomienda Batch por archivo mensual incorporado. |
| **Variety** | CSV tabular de nueve columnas, comprimido además en ZIP. Incluye timestamps, IDs, categorías, marcas, precios y sesiones. | No hay evidencia local de otra fuente o formato. Se conserva la semántica tabular, incluida la jerarquía de `category_code`. |
| **Veracity** | 35.413.780 `category_code` nulos (32,21 %), 15.331.243 `brand` nulos (13,94 %) y 12 `user_session` nulos. Cero faltantes en las otras seis columnas; cero fechas inválidas, filas corruptas detectadas, conversiones numéricas inválidas, IDs no positivos, espacios iniciales/finales o tipos de evento inesperados. | No descartar eventos por la falta de categoría o marca; conservarlos como categoría/atributo desconocido en una futura capa Silver. Excluir las 12 filas sin sesión de tasas por sesión. Los ceros de precio y valores extremos se marcan para análisis, no se eliminan automáticamente. |
| **Value** | 23.005.603 sesiones con vista válida; 2.304.215 con carrito posterior a la vista; 932.315 con compra posterior al carrito. Los resultados también describen categorías y horas/días de compra. | Las métricas permiten priorizar el paso del funnel, el foco de merchandising y horarios para campañas. El equipo comercial usaría estos indicadores como orientación para investigar acciones, no como causalidad demostrada. |

### Frecuencia observada por tipo de evento

| Evento | Octubre | Noviembre | Total |
|---|---:|---:|---:|
| `view` | 40.779.399 | 63.556.110 | 104.335.509 |
| `cart` | 926.516 | 3.028.930 | 3.955.446 |
| `purchase` | 742.849 | 916.939 | 1.659.788 |
| `remove_from_cart` | 0 | 0 | 0 |

La guía enumera `remove_from_cart` como evento posible, pero no aparece en los dos CSV observados. No se asume que falte en los siete meses completos.

## Calidad, duplicados y valores extremos

- **Nulos:** `category_code` falta en 35.413.780 filas (32,21 %); `brand`, en 15.331.243 (13,94 %); `user_session`, en 12 filas. El resto de las columnas no tiene faltantes en estos archivos.
- **Inconsistencias de formato:** no se detectaron timestamps inválidos, registros CSV corruptos, valores numéricos no convertibles, IDs no positivos, espacios iniciales/finales ni valores de `event_type` fuera del dominio observado.
- **Duplicados:** 75.652 grupos de filas con huella repetida y 130.739 filas excedentes. Se usó SHA-256 sobre una serialización canónica de las nueve columnas y el campo de registro corrupto; la posibilidad de colisión criptográfica es despreciable. El script informa explícitamente este método y no borra duplicados.
- **Precio:** los 109.950.743 valores se pudieron convertir a número. El mínimo es 0 y hay 256.761 precios no positivos (0,23 %). Cuantiles aproximados (error relativo configurado 1 %): Q1 = 66,93; mediana = 162,40; Q3 = 358,57. La cerca superior IQR es 796,03 y hay 9.710.837 candidatos estadísticos a outlier (8,83 %), con máximo 2.574,07. Son candidatos, no errores confirmados; no se recortaron ni eliminaron.
- **Sesiones:** 109.950.731 filas tienen `user_session` no vacío (99,99999 %). La tasa por sesión usa las sesiones con vista y timestamp válido; no mezcla usuarios sin sesión ni eventos con tiempo inválido.

## Preguntas de negocio y resultados observados

**Problema:** el e-commerce quiere entender cómo convertir más visitas a productos en compras.

1. **¿Qué proporción de sesiones pasa de ver productos a agregarlos al carrito y luego comprarlos?**
   - Vista → carrito posterior: 2.304.215 / 23.005.603 = **10,02 %**.
   - Vista → compra posterior al carrito: 932.315 / 23.005.603 = **4,05 %**.
   - Carrito → compra posterior: 932.315 / 2.304.215 = **40,46 %**.
   - **Decisión:** investigar el paso de vista a carrito primero; validar con el equipo comercial/UX antes de implementar cambios.

2. **¿Qué categorías concentran más compras y mayor valor estimado de compra?**
   - `electronics.smartphone`: 720.665 eventos `purchase`; suma de `price` = 334.871.284,98.
   - Categoría faltante: 407.643 eventos; suma = 52.805.443,81.
   - Suma de `price` en todos los eventos `purchase`: 505.152.392,77.
   - **Decisión:** usar estos resultados para decidir qué categorías priorizar en merchandising/promociones y mejorar la cobertura de `category_code`. La suma de precios es una aproximación asociada a eventos, no ingresos netos ni margen.

3. **¿En qué días y horas se registran más compras y mejor conversión?**
   - Se observan más eventos `purchase` a las **09:00 UTC** (126.617); más sesiones con vista que terminan en compra a las **10:00 UTC** (5,14 % de las sesiones con vista de esa hora).
   - Domingo registra la mayor conversión vista-compra en esta ventana (**6,59 %**); el viernes, la menor (**3,23 %**). Los conteos de compras sin normalizar son mayores el domingo (353.614).
   - **Decisión:** usar estas horas/días como hipótesis inicial para calendarizar campañas y validarlas con periodos adicionales. Son observaciones de dos meses y no demuestran causalidad ni estacionalidad anual.

## Limitaciones y uso de resultados

- Solo están disponibles octubre y noviembre de 2019, no los siete meses completos.
- Los datos de `event_time` representan cuándo se registró el evento, no cuándo el archivo llegó a un sistema. Los conteos horarios no describen una latencia online.
- La compra se mide por eventos y sesiones de clickstream; no hay datos de órdenes consolidadas, ingresos contables, costos, margen, inventario ni campañas.
- Los nulos de categoría y marca, los ceros de precio y los candidatos IQR requieren reglas de negocio antes de cualquier limpieza. Esta Fase 1 solo los identifica.
- Los conteos y tasas son evidencia observacional para responder las preguntas propuestas; no son pruebas causales ni predicciones.
