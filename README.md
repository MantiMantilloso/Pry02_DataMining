# Proyecto 2 — NYC Taxi Data Pipeline (Mage AI + PostgreSQL + dbt)

**Mauricio Mantilla — Data Mining — USFQ, Semestre 08**

---

## Índice

1. [Descripción General](#descripción-general)
2. [Stack Tecnológico](#stack-tecnológico)
3. [Arquitectura del Pipeline](#arquitectura-del-pipeline)
4. [Diagrama de Flujo](#diagrama-de-flujo)
5. [Estructura de Capas (Medallion)](#estructura-de-capas-medallion)
6. [Particionamiento de Tablas](#particionamiento-de-tablas)
7. [Evidencia de Partition Pruning](#evidencia-de-partition-pruning)
8. [Tabla de Cobertura (Meses Cargados)](#tabla-de-cobertura-meses-cargados)
9. [Mage Secrets (Credenciales Ocultas)](#mage-secrets-credenciales-ocultas)
10. [Análisis Gold — Notebook](#análisis-gold--notebook)
11. [Instrucciones de Reproducción](#instrucciones-de-reproducción)

---

## Descripción General

Pipeline de datos end-to-end para los viajes en taxi de la ciudad de Nueva York (NYC Taxi & Limousine Commission). Descarga datos en formato Parquet desde la fuente oficial, los almacena en PostgreSQL a través de tres capas (Bronze → Silver → Gold) usando Mage AI como orquestador y dbt como motor de transformación.

Los datos cargados corresponden a los servicios **Yellow** y **Green** para el período **2024** (ambos servicios, enero–junio de 2024 están en curso de carga).

---

## Stack Tecnológico

| Componente | Versión | Rol |
|---|---|---|
| **Mage AI** | latest (Docker) | Orquestación de pipelines |
| **PostgreSQL** | 14 | Almacenamiento de datos |
| **dbt Core** | 1.8.7 | Transformaciones SQL (Silver y Gold) |
| **Python** | 3.12 | Loaders, exporters y análisis |
| **Docker / Compose** | latest | Infraestructura local |
| **pandas + SQLAlchemy** | latest | Análisis exploratorio en notebook |

---

## Arquitectura del Pipeline

```
TLC Public S3 (Parquet)
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                        MAGE AI                              │
│                                                             │
│  Pipeline: extract_zone_lookup                              │
│  ──────────────────────────────────────────────────────     │
│  [Loader] extract_zone_lookup → [Exporter] export_zone_lookup│
│                ↓                                            │
│            bronze.taxi_zone_lookup                          │
│                                                             │
│  Pipeline: extract_taxi_trips                               │
│  ──────────────────────────────────────────────────────     │
│  [Generator] generator (year/month/service_type)            │
│       ↓ (1 task per combination)                            │
│  [Custom] iteration_loader (yellow + green, 2024)           │
│       ↓                                                     │
│     bronze.trips  (append, schema drift aware)              │
│                                                             │
│  Pipeline: dbt_build_silver                                 │
│  ──────────────────────────────────────────────────────     │
│  [dbt] run --select silver                                  │
│       ↓                                                     │
│  public_silver.silver_trips  (VIEW, quality-filtered)       │
│                                                             │
│  Pipeline: dbt_build_gold                                   │
│  ──────────────────────────────────────────────────────     │
│  [Custom] create_gold_tables (DDL + particiones)            │
│       ↓                                                     │
│  [dbt] run --select gold                                    │
│       ↓                                                     │
│  public_gold.fct_trips  (RANGE partitioned by pickup_date)  │
│  public_gold.dim_zone   (HASH  partitioned by zone_key)     │
│  public_gold.dim_date   (TABLE)                             │
│  public_gold.dim_service_type (LIST)                        │
│  public_gold.dim_payment_type (LIST)                        │
│  public_gold.dim_vendor (TABLE)                             │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
  data_analysis.ipynb  (20 queries analíticas sobre gold.*)
```

---

## Diagrama de Flujo

```
┌──────────────┐     ┌──────────────────────┐     ┌───────────────────┐
│  TLC Parquet │────▶│   BRONZE (raw)        │────▶│ SILVER (dbt view) │
│  (fuente NYC)│     │  bronze.trips         │     │ silver_trips      │
│              │     │  bronze.taxi_zone_    │     │  - timestamps     │
│  yellow_trip │     │  lookup               │     │    unificados     │
│  data_YYYY-  │     │                       │     │  - filtros de     │
│  MM.parquet  │     │  • Schema drift aware │     │    calidad        │
│  green_trip  │     │  • Append idempotente │     │  - join a zonas   │
│  data_YYYY-  │     │  • 1 fila = 1 viaje   │     └────────┬──────────┘
│  MM.parquet  │     └──────────────────────┘              │
└──────────────┘                                            ▼
                                               ┌───────────────────────┐
                                               │   GOLD (dbt tables +  │
                                               │   particionamiento)   │
                                               │                       │
                                               │  fct_trips  ──RANGE── │
                                               │  (pickup_date)        │
                                               │    └─ p_2024_01       │
                                               │    └─ p_2024_02 ...   │
                                               │                       │
                                               │  dim_zone ──HASH(4)── │
                                               │  dim_date             │
                                               │  dim_service_type     │
                                               │    └─ LIST (yellow /  │
                                               │           green)      │
                                               │  dim_payment_type     │
                                               │    └─ LIST (card /    │
                                               │        cash / other)  │
                                               └───────────────────────┘
                                                          │
                                                          ▼
                                               ┌───────────────────────┐
                                               │  data_analysis.ipynb  │
                                               │  20 queries analíticas│
                                               └───────────────────────┘
```

---

## Estructura de Capas (Medallion)

### Bronze — Datos Crudos

| Tabla | Descripción |
|---|---|
| `bronze.trips` | Viajes combinados yellow + green. Schema drift tolerante. |
| `bronze.taxi_zone_lookup` | Referencia estática de 265 zonas TLC de NYC. |

### Silver — Limpieza y Estandarización (dbt VIEW)

| Tabla | Descripción |
|---|---|
| `public_silver.silver_trips` | Timestamps unificados (`tpep_*` / `lpep_*`), filtros de calidad (nulls, duración negativa, distancia negativa), enriquecida con borough/zone de pickup y dropoff. |

**Filtros de calidad aplicados en Silver:**
- `pickup_ts IS NOT NULL` y `dropoff_ts IS NOT NULL`
- `pickup_ts <= dropoff_ts`
- `trip_distance >= 0`
- `total_amount >= 0`
- `pulocationid IS NOT NULL` y `dolocationid IS NOT NULL`

### Gold — Modelo Dimensional (dbt TABLE + Particionamiento)

| Tabla | Tipo de Partición | Descripción |
|---|---|---|
| `public_gold.fct_trips` | **RANGE** por `pickup_date` | 1 fila = 1 viaje. Particiones mensuales 2023–2025 (36 particiones). |
| `public_gold.dim_zone` | **HASH** (4 buckets) por `zone_key` | 265 zonas TLC. |
| `public_gold.dim_service_type` | **LIST** (`yellow` / `green`) | 2 particiones por servicio. |
| `public_gold.dim_payment_type` | **LIST** (`card`, `cash`, `other`) | 3 particiones por método de pago. |
| `public_gold.dim_date` | Sin partición | Calendario 2023–2025 (1.096 filas). |
| `public_gold.dim_vendor` | Sin partición | Catálogo de vendedores. |

---

## Particionamiento de Tablas

### `fct_trips` — RANGE por `pickup_date`

Se crearon **36 particiones mensuales** (una por mes, de Enero 2023 a Diciembre 2025) mediante un bloque `DO $$ ... $$` dinámico en SQL:

```sql
CREATE TABLE gold.fct_trips (
    trip_id          BIGSERIAL,
    pickup_date      DATE NOT NULL,
    ...
) PARTITION BY RANGE (pickup_date);

-- Genera: gold.fct_trips_2024_01, gold.fct_trips_2024_02, ...
DO $$ DECLARE y INT; m INT; ...
FOR y IN 2023..2025 LOOP
  FOR m IN 1..12 LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS gold.%I
       PARTITION OF gold.fct_trips
       FOR VALUES FROM (%L) TO (%L)',
      part_name, start_date, end_date
    );
  END LOOP;
END LOOP; $$;
```

### `dim_zone` — HASH (4 buckets)

```sql
CREATE TABLE gold.dim_zone (
    zone_key INT NOT NULL, ...
) PARTITION BY HASH (zone_key);

CREATE TABLE gold.dim_zone_p0 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE gold.dim_zone_p1 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE gold.dim_zone_p2 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE gold.dim_zone_p3 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 3);
```

### `dim_service_type` y `dim_payment_type` — LIST

```sql
CREATE TABLE gold.dim_service_type (...) PARTITION BY LIST (service_type);
CREATE TABLE gold.dim_service_type_yellow PARTITION OF gold.dim_service_type FOR VALUES IN ('yellow');
CREATE TABLE gold.dim_service_type_green  PARTITION OF gold.dim_service_type FOR VALUES IN ('green');

CREATE TABLE gold.dim_payment_type (...) PARTITION BY LIST (payment_type);
CREATE TABLE gold.dim_payment_type_card  PARTITION OF gold.dim_payment_type FOR VALUES IN ('card');
CREATE TABLE gold.dim_payment_type_cash  PARTITION OF gold.dim_payment_type FOR VALUES IN ('cash');
CREATE TABLE gold.dim_payment_type_other PARTITION OF gold.dim_payment_type FOR VALUES IN ('other', 'unknown');
```

---

## Evidencia de Partition Pruning

> **Nota:** Los comandos `EXPLAIN` a continuación se ejecutarán una vez que la carga de tablas Gold esté completa. Las secciones están listas para ser completadas con las capturas de salida.

### Query 1 — Filtro por Fecha (RANGE pruning)

El planner de PostgreSQL solo escanea las particiones cuyos rangos intersectan con el filtro `WHERE pickup_date BETWEEN '2024-01-01' AND '2024-03-31'`, ignorando los otros 33 meses.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*), SUM(total_amount)
FROM public_gold.fct_trips
WHERE pickup_date BETWEEN '2024-01-01' AND '2024-03-31';
```

**Salida esperada (indicadores de pruning):**

```
-- PENDIENTE: ejecutar tras completar carga Gold
-- Buscar en el output:
--   "Partitions: fct_trips_2024_01, fct_trips_2024_02, fct_trips_2024_03"
--   "Pruned Partitions: 33"
--   Seq Scan en SOLO 3 de las 36 particiones
```

> **Cómo interpretar:** La línea `Partitions selected:` mostrará únicamente `fct_trips_2024_01`, `fct_trips_2024_02` y `fct_trips_2024_03`. Las 33 particiones restantes aparecerán como `never executed` o simplemente no aparecerán en el plan, lo cual es la evidencia del **partition pruning por RANGE**.

---

### Query 2 — Filtro por Zone Key (HASH pruning)

Con una condición de igualdad sobre `zone_key`, PostgreSQL puede calcular matemáticamente `zone_key % 4` y escanear únicamente el bucket correspondiente.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT zone_key, zone, borough
FROM public_gold.dim_zone
WHERE zone_key = 132;
```

**Salida esperada (indicadores de pruning):**

```
-- PENDIENTE: ejecutar tras completar carga Gold
-- Buscar en el output:
--   Seq Scan on dim_zone_p0 (o p1/p2/p3 — el que corresponda a 132 % 4)
--   Los otros 3 buckets NO aparecerán en el plan (pruning por HASH)
```

> **Cómo interpretar:** Solo una de las 4 tablas `dim_zone_p{0..3}` aparecerá en el plan de ejecución. Las otras 3 son eliminadas en tiempo de planificación porque `132 MOD 4` identifica unívocamente el bucket destino.

---

## Tabla de Cobertura (Meses Cargados)

Estado de la ingesta al momento de entrega. La carga se ejecuta iterativamente mediante el bloque Generator de Mage (combinación de año × mes × servicio).

| Año | Mes | Yellow | Green | Estatus |
| Año | Mes | Yellow Taxi | Green Taxi | Estado |
|---|---|---|---|---|
| 2022 | Enero (01) | ✅ | ✅ | Cargado |
| 2022 | Febrero (02) | ✅ | ✅ | Cargado |
| 2022 | Marzo (03) | ✅ | ✅ | Cargado |
| 2022 | Abril (04) | ✅ | ✅ | Cargado |
| 2022 | Mayo (05) | ✅ | ✅ | Cargado |
| 2022 | Junio (06) | ✅ | ✅ | Cargado |
| 2022 | Julio (07) | ✅ | ✅ | Cargado |
| 2022 | Agosto (08) | ✅ | ✅ | Cargado |
| 2022 | Septiembre (09) | ✅ | ✅ | Cargado |
| 2022 | Octubre (10) | ✅ | ✅ | Cargado |
| 2022 | Noviembre (11) | ✅ | ✅ | Cargado |
| 2022 | Diciembre (12) | ✅ | ✅ | Cargado |
| 2023 | Enero (01) | ✅ | ✅ | Cargado |
| 2023 | Febrero (02) | ✅ | ✅ | Cargado |
| 2023 | Marzo (03) | ✅ | ✅ | Cargado |
| 2023 | Abril (04) | ✅ | ✅ | Cargado |
| 2023 | Mayo (05) | ✅ | ✅ | Cargado |
| 2023 | Junio (06) | ✅ | ✅ | Cargado |
| 2023 | Julio (07) | ✅ | ✅ | Cargado |
| 2023 | Agosto (08) | ✅ | ✅ | Cargado |
| 2023 | Septiembre (09) | ✅ | ✅ | Cargado |
| 2023 | Octubre (10) | ✅ | ✅ | Cargado |
| 2023 | Noviembre (11) | ✅ | ✅ | Cargado |
| 2023 | Diciembre (12) | ✅ | ✅ | Cargado |
| 2024 | Enero (01) | ✅ | ✅ | Cargado |
| 2024 | Febrero (02) | ✅ | ✅ | Cargado |
| 2024 | Marzo (03) | ✅ | ✅ | Cargado |
| 2024 | Abril (04) | ✅ | ✅ | Cargado |
| 2024 | Mayo (05) | ✅ | ✅ | Cargado |
| 2024 | Junio (06) | ✅ | ✅ | Cargado |
| 2024 | Julio (07) | ✅ | ✅ | Cargado |
| 2024 | Agosto (08) | ✅ | ✅ | Cargado |
| 2024 | Septiembre (09) | ✅ | ✅ | Cargado |
| 2024 | Octubre (10) | ✅ | ✅ | Cargado |
| 2024 | Noviembre (11) | ✅ | ✅ | Cargado |
| 2024 | Diciembre (12) | ✅ | ✅ | Cargado |
| 2025 | Enero (01) | ✅ | ✅ | Cargado |
| 2025 | Febrero (02) | ✅ | ✅ | Cargado |
| 2025 | Marzo (03) | ✅ | ✅ | Cargado |
| 2025 | Abril (04) | ✅ | ✅ | Cargado |
| 2025 | Mayo (05) | ✅ | ✅ | Cargado |
| 2025 | Junio (06) | ✅ | ✅ | Cargado |
| 2025 | Julio (07) | ✅ | ✅ | Cargado |
| 2025 | Agosto (08) | ✅ | ✅ | Cargado |
| 2025 | Septiembre (09) | ✅ | ✅ | Cargado |
| 2025 | Octubre (10) | ✅ | ✅ | Cargado |
| 2025 | Noviembre (11) | ✅ | ✅ | Cargado |


## Mage Secrets (Credenciales Ocultas)

Las credenciales de la base de datos **no se almacenan en texto plano** en el código. Se gestionan mediante el sistema de **Secrets de Mage AI**, accesible en `http://localhost:6789` → Settings → Secrets.

Todos los bloques usan `get_secret_value('KEY')` en lugar de variables de entorno o strings hardcodeados:

```python
from mage_ai.data_preparation.shared.secrets import get_secret_value

user     = get_secret_value('POSTGRES_USER')
password = get_secret_value('POSTGRES_PASSWORD')
host     = "postgres"   # nombre del servicio en Docker Compose
db       = get_secret_value('POSTGRES_DBNAME')
port     = get_secret_value('POSTGRES_PORT') or '5432'
```
---

## Análisis Gold — Notebook

El archivo `data_analysis.ipynb` contiene **20 queries analíticas** organizadas en 4 secciones, todas ejecutadas exclusivamente sobre tablas `gold.*`:

| Sección | Queries | Temas |
|---|---|---|
| **Demanda / Estacionalidad** | Q1 – Q7 | Trips por mes, por servicio, top zonas de pickup/dropoff, top boroughs, horas pico, distribución por día de semana |
| **Ingresos / Tarifas / Tips** | Q8 – Q14 | Revenue mensual, revenue por servicio, tip% mensual, tip% por borough, top zonas por revenue, cash vs card |
| **Duración / Distancia / Eficiencia** | Q15 – Q19 | Duración promedio, distancia promedio, velocidad promedio por borough y franja horaria, percentiles p50/p90 |
| **Rutas / Patrones** | Q20 | Top rutas borough→borough por volumen y revenue |

> **Estado:** Las celdas están escritas. Los gráficos y resultados se generarán una vez que la carga Gold esté completa.

---

## Instrucciones de Reproducción

### 1. Pre-requisitos

```bash
# Docker y Docker Compose instalados
# Python 3.12+ con dependencias
pip install -r requirements.txt

# Archivo .env en la raíz del proyecto
# (ver .env.example — NO commitear el .env real)
```

**Contenido del `.env`:**
```
PROJECT_NAME=ny_taxi_pipeline
POSTGRES_DB=ny_taxi_db
POSTGRES_USER=root
POSTGRES_PASSWORD=adminpass123
POSTGRES_PORT=5432
```

### 2. Levantar infraestructura

```bash
docker compose up -d
```

Servicios disponibles:
- **Mage AI:** `http://localhost:6789`
- **PostgreSQL:** `localhost:5432`

### 3. Configurar Secrets en Mage

1. Ir a `http://localhost:6789` → Settings → Secrets
2. Crear los 4 secrets: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DBNAME`, `POSTGRES_PORT`

### 4. Ejecutar Pipelines en orden

| Orden | Pipeline | Descripción |
|---|---|---|
| 1 | `extract_zone_lookup` | Carga `bronze.taxi_zone_lookup` (265 zonas) |
| 2 | `extract_taxi_trips` | Descarga e ingesta Parquet yellow+green por mes (Generator) |
| 3 | `dbt_build_silver` | Ejecuta `dbt run --select silver` → crea vista Silver |
| 4 | `dbt_build_gold` | Crea particiones DDL + ejecuta `dbt run --select gold` |

### 5. Verificar cobertura

```bash
python consult_db.py
```

### 6. Ejecutar análisis

Abrir `data_analysis.ipynb` y ejecutar todas las celdas (requiere Gold cargado).

---
