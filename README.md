# Pipeline ETL: Relación entre Goles de Messi, Apple (AAPL) y Netflix (NFLX)

Este proyecto implementa una arquitectura ETL orientada a objetos en Python para contrastar la hipótesis de si **los goles anotados por Lionel Messi en la Major League Soccer (MLS) afectan las cotizaciones de cierre de Apple Inc. (AAPL) y Netflix Inc. (NFLX)** durante el período **2023 a 2026**.

---

## 🗄️ Estructura del Proyecto

```text
pipeline_messi_apple/
├── Base-0/                              # Respaldo de versiones iniciales
├── datos/                               # Carpetas por activo administradas por diccionario
│   ├── apple/
│   │   └── acciones_apple_intermedio.pkl    # Intermedio de Apple (E_Obtener)
│   ├── netflix/
│   │   └── acciones_netflix_intermedio.pkl  # Intermedio de Netflix (E_Obtener)
│   └── messi/
│       └── goles_messi_intermedio.pkl       # Fechas de goles en MLS filtradas (E_Obtener)
├── datos_goles_messi.csv                # Fuente CSV de goles origen
├── datos_messi_apple_netflix.pkl        # Dataset final consolidado (L_Cargar)
├── grafico_messi_apple_netflix.png      # Gráfico conjunto final
├── pipeline_messi_apple.py              # Script principal con las 4 clases
└── README.md
```

---

## 🏗️ Nuevas Clases y Flujo de Procesamiento

```mermaid
flowchart TD
    subgraph Extracto [1. Fase de Extracción - E_Obtener]
        DICT[self.activos: Dict de Tickers y Carpetas] -->|Pasa elemento del dict sin validar string| O1[obtener_datos_polygon]
        O1 --> P1[(datos/apple/acciones_apple_intermedio.pkl)]
        O1 --> P2[(datos/netflix/acciones_netflix_intermedio.pkl)]
        CSV[datos_goles_messi.csv] -->|Filtro MLS directo en la extracción| O2[extraer_goles_messi_intermedio]
        O2 --> P3[(datos/messi/goles_messi_intermedio.pkl)]
    end

    subgraph Transformacion [2. Fase de Transformación - T_Transformar]
        P1 --> T1[rellenar_datos_faltantes_apple: Aproximación lineal sobre NaNs]
        P2 --> T2[rellenar_datos_faltantes_netflix: Aproximación lineal sobre NaNs]
        P3 --> T3[rellenar_datos_faltantes_messi]
        T3 --> T4[Groupby de Pandas sobre fechas sumando goles]
        T1 --> T5[Cruce con valores de CIERRE de Apple]
        T2 --> T6[Cruce con valores de CIERRE de Netflix]
        T4 --> T7[Integración Final Homogenizada int8/int32]
        T5 --> T7
        T6 --> T7
    end

    subgraph CargaFinal [3. Persistencia Final - L_Cargar]
        T7 --> L1[guardar_datos_finales]
        L1 --> PKL[(datos_messi_apple_netflix.pkl)]
    end

    subgraph Orquestacion [4. Orquestación y Gráfico - PipelineETL]
        PKL --> G1[cargar_datos_finales]
        G1 --> G2[Gráfico Conjunto]
        G2 --> G3[Barras: Goles sumados por fecha]
        G2 --> G4[Líneas Superpuestas: Valores de CIERRE AAPL y NFLX]
    end
```

---

### 1. `E_Obtener`
- **Filtro directo de MLS:** El método `extraer_goles_messi_intermedio()` filtra directamente sobre el CSV únicamente las fechas de los goles de Messi en la MLS en el período 2023–2026 antes de guardar el archivo intermedio.
- **Gestión por Diccionario:** Posee `self.activos` con los tickers y carpetas. El método `obtener_datos_polygon(activo)` recibe directamente un elemento del diccionario sin requerir comprobación de strings.
- **Métodos de archivo intermedio:** `guardar_intermedio()` y `obtener_intermedio()` pertenecen a `E_Obtener`.

### 2. `T_Transformar`
- **Aproximaciones Lineales sobre NaNs:** Los métodos `rellenar_datos_faltantes_apple()` y `rellenar_datos_faltantes_netflix()` generan el calendario de días hábiles, combinan los datos obtenidos en `E_Obtener` y rellenan los valores faltantes mediante **aproximaciones lineales** (`interpolate(method='linear')`) entre los días que tienen valor.
- **Groupby de Pandas:** En `rellenar_datos_faltantes_messi()`, agrupa los goles sumándolos por fecha (`Goles_Messi = sum`).
- **Valores de Cierre:** Cruza los partidos con las cotizaciones de **cierre** de Apple y Netflix.
- **Optimización de RAM:** Homogeniza los tipos de número entero a `int8` (goles) y `int32` (volumen).

### 3. `L_Cargar`
- Gestiona la persistencia y recuperación del dataset final serializado en [`datos_messi_apple_netflix.pkl`](file:///d:/Git/pipeline_messi_apple/datos_messi_apple_netflix.pkl).

### 4. `PipelineETL`
- Orquesta las 3 clases y genera el gráfico [`grafico_messi_apple_netflix.png`](file:///d:/Git/pipeline_messi_apple/grafico_messi_apple_netflix.png) combinando:
  - **Barras:** Total de goles de Messi anotados por fecha en la MLS.
  - **Líneas Superpuestas:** Valores de **cierre** en dólares ($ USD) de Apple (azul) y Netflix (rojo).

---

## 📊 Resultados Estadísticos (2023 - 2026)

| Activo Bursátil | Métrica Evaluada | Coeficiente $R^2$ | P-valor ($p$) | Veredicto |
| :--- | :--- | :--- | :--- | :--- |
| **Apple (AAPL)** | Valor de Cierre vs Goles | $0.0054$ ($0.54\%$) | $0.6509$ | ❌ Hipótesis Refutada |
| **Netflix (NFLX)** | Valor de Cierre vs Goles | $0.0054$ ($0.54\%$) | $0.6509$ | ❌ Hipótesis Refutada |

---

## 🚀 Ejecución

```powershell
python pipeline_messi_apple.py
```
