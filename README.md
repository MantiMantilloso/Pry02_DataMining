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

> **Validación:** El partition pruning fue verificado experimentalmente. Las queries que filtran directamente sobre `f.pickup_date` (RANGE key) reducen el escaneo de 36 a 12 particiones para el año 2024, logrando ~67% de reducción. Las queries que filtran vía `d.year = 2024` en la tabla de dimensión **no** activan partition pruning — por eso todas las queries del notebook usan el filtro directo sobre la fact table.

### Query 1 — Filtro por Fecha (RANGE pruning)

El planner de PostgreSQL solo escanea las particiones cuyos rangos intersectan con el filtro `WHERE pickup_date`, ignorando los meses fuera del rango.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*), SUM(total_amount)
FROM public_gold.fct_trips
WHERE pickup_date BETWEEN '2024-01-01' AND '2024-03-31';
```

**Resultado:** PostgreSQL escanea únicamente las particiones `fct_trips_2024_01`, `fct_trips_2024_02` y `fct_trips_2024_03`. Las 33 particiones restantes aparecen como `never executed` o no aparecen en el plan — evidencia directa del **partition pruning por RANGE**.

> **Nota clave para el notebook:** Usar `WHERE f.pickup_date >= '2024-01-01' AND f.pickup_date < '2025-01-01'` en lugar de `WHERE d.year = 2024` es lo que activa el pruning, reduciendo de 36 a 12 particiones escaneadas.

---

### Query 2 — Filtro por Zone Key (HASH pruning)

Con una condición de igualdad sobre `zone_key`, PostgreSQL calcula `zone_key % 4` y escanea únicamente el bucket correspondiente.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT zone_key, zone, borough
FROM public_gold.dim_zone
WHERE zone_key = 132;
```

**Resultado:** Solo una de las 4 tablas `dim_zone_p{0..3}` aparece en el plan de ejecución. Las otras 3 son eliminadas en tiempo de planificación porque `132 MOD 4` identifica unívocamente el bucket destino — evidencia del **partition pruning por HASH**.

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

El archivo `data_analysis.ipynb` contiene **20 queries analíticas** organizadas en 4 secciones, todas ejecutadas exclusivamente sobre tablas `gold.*`.

> **Optimización aplicada:** Las queries que filtran por año 2024 usan `WHERE f.pickup_date >= '2024-01-01' AND f.pickup_date < '2025-01-01'` directamente sobre la columna de partición de `fct_trips`, lo que habilita **partition pruning** y reduce el escaneo de 36 a 12 particiones.

---

### Sección 1 — Demanda / Estacionalidad (Q1–Q7)

| Query | Título | Hallazgo Principal |
|---|---|---|
| **Q1** | Trips per month (2024) | El volumen mensual muestra estacionalidad clara — primavera y otoño tienden a picos mientras agosto baja, reflejando patrones de turismo y commuters en NYC. |
| **Q2** | Trips by service_type & month | Yellow taxis dominan consistentemente el volumen en todos los meses. Green taxis muestran demanda baja y plana, concentrada en outer boroughs. |
| **Q3** | Top 10 pickup zones | Zonas de Midtown Manhattan (Penn Station, Times Sq, Grand Central) y aeropuertos JFK/LaGuardia dominan los pickups. |
| **Q4** | Top 10 dropoff zones | Los dropoffs replican el patrón de pickups, confirmando que Midtown y aeropuertos son hubs de origen y destino. Simetría sugiere patrones de ida y vuelta. |
| **Q5** | Top 5 boroughs per month | Manhattan es #1 consistentemente por amplio margen. Queens #2 (tráfico de aeropuertos). Brooklyn, Bronx y Staten Island siguen con ranking estable. |
| **Q6** | Peak hours per day of week | Horas pico en días laborales: 18:00–20:00 (commute nocturno). Fines de semana: picos más tarde (22:00–02:00, vida nocturna). Rush matutino (8–9am) es pico secundario. |
| **Q7** | Trip distribution by day of week | Viernes y sábado son los días más activos (nightlife). Domingo sorprendentemente fuerte. Lunes el más tranquilo (trabajo remoto). |

---

### Sección 2 — Ingresos / Tarifas / Tips (Q8–Q14)

| Query | Título | Hallazgo Principal |
|---|---|---|
| **Q8** | Total revenue per month | Revenue sigue de cerca el volumen de viajes (picos en primavera/otoño). La tarifa promedio por viaje es estable, por lo que el volumen es el driver principal de revenue. |
| **Q9** | Revenue by service_type & month | Yellow taxis generan la gran mayoría del revenue. Green taxis contribuyen una cuota pequeña pero consistente, con tarifas promedio ligeramente más altas por viajes más largos en outer boroughs. |
| **Q10** | Avg tip % per month | El porcentaje de propina se mantiene estable alrededor de 15–20%, con variación estacional menor. Meses de invierno pueden mostrar propinas ligeramente más altas (generosidad navideña / mal clima). |
| **Q11** | Tip % by borough & month | Zonas de aeropuerto (Queens/EWR) tienden a mayor tip%. Outer boroughs con más uso de efectivo muestran promedios más bajos ya que propinas en cash no se registran en el dataset. |
| **Q12** | Top 10 zones by revenue (pickup) | JFK Airport y zonas de Midtown lideran el revenue. Viajes a aeropuertos son largos y costosos, generando valor desproporcionado por viaje. |
| **Q13** | Top 10 zones by tip % (min 500 trips) | Con N=500 se elimina ruido de zonas de bajo volumen. Top zonas por tip% tienden a estar cerca de hoteles y aeropuertos (pago con tarjeta + turistas generosos). |
| **Q14** | Cash vs Card: trips, revenue, tip % | Pagos con tarjeta representan la mayoría de viajes y muestran tip% mucho más alto. Cash tip% es cercano a cero porque las propinas en efectivo no se capturan — se dan directamente al conductor. |

---

### Sección 3 — Duración / Distancia / Eficiencia (Q15–Q19)

| Query | Título | Hallazgo Principal |
|---|---|---|
| **Q15** | Avg trip duration per month | Duración promedio estable en 12–16 minutos. Incrementos leves en invierno por tráfico más lento debido a condiciones climáticas. |
| **Q16** | Avg trip distance per month | Distancia estable alrededor de 3–4 millas, típico de short-haul en NYC. Meses de verano pueden mostrar viajes ligeramente más largos (turistas a atracciones externas). |
| **Q17** | Avg speed by borough & time slot | Late night / early morning muestran las velocidades más altas (menos tráfico). Rush hours matutino y vespertino en Manhattan muestran las velocidades más bajas (congestión severa). |
| **Q18** | P50 and P90 duration by borough | La brecha p50–p90 es mayor en outer boroughs, indicando alta varianza — la mayoría de viajes son cortos pero existe una cola significativa de viajes muy largos (aeropuertos, cross-borough). |
| **Q19** | Top 10 zones by P90 duration | Zonas con p90 más alto tienden a estar en outer boroughs o cerca de aeropuertos, donde los viajes cruzan múltiples boroughs. Representan los pickups más intensivos en tiempo para conductores. |

---

### Sección 4 — Rutas / Patrones (Q20)

| Query | Título | Hallazgo Principal |
|---|---|---|
| **Q20** | Top 10 borough→borough routes | Manhattan→Manhattan es la ruta más común por amplio margen. Rutas cross-borough como Manhattan→Queens (aeropuerto) y Manhattan→Brooklyn representan los flujos inter-borough más significativos. |

---

### Resumen de Hallazgos Clave

1. **Dominancia de Manhattan:** Tanto en pickups, dropoffs como en revenue, Manhattan concentra la mayor actividad taxi.
2. **Yellow >> Green:** Yellow taxis dominan en volumen y revenue; green taxis operan un nicho estable en outer boroughs.
3. **Estacionalidad:** Primavera y otoño son picos; agosto muestra una baja. Viernes y sábados son los días más activos.
4. **Aeropuertos como generadores de valor:** JFK y LaGuardia generan revenue desproporcionado por viaje (viajes largos y costosos) y muestran tip% más altos.
5. **Efecto del método de pago:** Las propinas en tarjeta (15–20%) dominan; las propinas en efectivo no se registran, subestimando la generosidad real.
6. **Congestión urbana:** Velocidades promedio caen significativamente durante rush hours en Manhattan, mientras que late night muestra las velocidades más altas.

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
