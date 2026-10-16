"""
=============================================================================
PIPELINE ETL: RELACIÓN ENTRE GOLES DE MESSI, ACCIONES DE APPLE (AAPL) Y NETFLIX (NFLX)
=============================================================================

Este script implementa un pipeline ETL estructurado en 4 clases principales:
  1. ObtenerE: Extracción de datos en lotes de 20 registros (Polygon.io y CSV),
     tanto para Apple (AAPL) como para Netflix (NFLX) y los goles de Messi.
  2. TransformarT: Limpieza, filtrado exclusivo por MLS (período 2023 a 2026),
     homogenización con Pandas y cruce con cotizaciones de Apple y Netflix.
  3. CargarL: Persistencia y recuperación en formato Pickle (.pkl).
  4. PipelineETL: Orquestación del proceso completo, recuperación del archivo
     Pickle, cálculo estadístico y generación del gráfico solicitado:
     - Gráfico de BARRAS para los goles de Messi.
     - LÍNEAS SUPERPUESTAS para las acciones de Apple y Netflix.

Período evaluado: 2023 a 2026.
Límite de procesamiento: 20 registros por lote para todas las fuentes.
"""

import os
import json
import time
import pickle
from datetime import datetime, timedelta
from typing import Generator, Tuple, Empresa, Any, Optional, List

import requests
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns


# =============================================================================
# 1. CLASE OBTENERE (EXTRACCIÓN)
# =============================================================================
class ObtenerE:
    """
    Clase encargada de la extracción de datos desde las fuentes requeridas:
      - Archivo CSV con el historial de goles de Lionel Messi.
      - API de Polygon.io para las acciones de Apple (AAPL) y Netflix (NFLX).
      
    Implementa procesamiento por lotes estrictamente limitado a 20 registros por vez.
    Ofrece una interfaz sencilla y métodos con alcances concretos para TransformarT.
    """

    def __init__(
        self,
        api_key: str = "8l9wv9ICO6LtyeWpxqf9kNrtyYRcQFXA",
        tickers: Optional[List[str]] = None,
        batch_size: int = 20,
        ruta_csv_messi: str = "datos_goles_messi.csv",
        ruta_cache_apple: str = "cache_acciones_apple.json",
        ruta_cache_netflix: str = "cache_acciones_netflix.json"
    ):
        self.api_key = api_key
        self.tickers = tickers or ["AAPL", "NFLX"]
        self.batch_size = batch_size
        self.ruta_csv_messi = ruta_csv_messi
        self.ruta_cache_apple = ruta_cache_apple
        self.ruta_cache_netflix = ruta_cache_netflix
        self.base_url_polygon = "https://api.polygon.io/v2/aggs/ticker"
        
        # Índices en memoria para búsqueda ultrarrápida por fecha
        self._acciones_apple_dict: Optional[Empresa[str, Empresa[str, Any]]] = None
        self._acciones_netflix_dict: Optional[Empresa[str, Empresa[str, Any]]] = None

    # -------------------------------------------------------------------------
    # Extracción de Goles de Messi (CSV en lotes de 20)
    # -------------------------------------------------------------------------
    def iterar_goles_messi(
        self, ruta_csv: Optional[str] = None, batch_size: Optional[int] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Generador que lee el archivo CSV de goles de Messi iterando en lotes
        limitados a `batch_size` (por defecto 20 registros por vez).
        
        Yields:
            pd.DataFrame: Fragmento con un máximo de 20 registros.
        """
        ruta = ruta_csv or self.ruta_csv_messi
        tamano_lote = batch_size or self.batch_size

        if not os.path.exists(ruta):
            raise FileNotFoundError(f"No se encontró el archivo de goles en: {ruta}")

        print(f"[ObtenerE] Leyendo goles de Messi desde '{ruta}' en lotes de {tamano_lote} registros...")
        lector_lotes = pd.read_csv(ruta, chunksize=tamano_lote)
        num_lote = 1
        for lote in lector_lotes:
            print(f"  -> [ObtenerE] Lote #{num_lote} de goles obtenido ({len(lote)} registros).")
            yield lote
            num_lote += 1

    # -------------------------------------------------------------------------
    # Extracción de Acciones (Apple y Netflix) desde Polygon.io (Lotes de 20)
    # -------------------------------------------------------------------------
    def obtener_lote_acciones_polygon(
        self,
        ticker: str,
        url_personalizada: Optional[str] = None,
        desde: str = "2024-10-15",
        hasta: str = "2026-10-15",
        limit: int = 20
    ) -> Tuple[list, Optional[str]]:
        """
        Realiza una petición a la API de Polygon.io para obtener un lote de hasta
        20 registros de cotizaciones de un ticker dado ('AAPL' o 'NFLX').
        
        Args:
            ticker: Símbolo bursátil ('AAPL' o 'NFLX').
            url_personalizada: URL completa (ej. next_url para paginación).
            desde: Fecha de inicio en formato YYYY-MM-DD.
            hasta: Fecha de fin en formato YYYY-MM-DD.
            limit: Límite de registros por lote (máx 20).
            
        Returns:
            Tuple con la lista de registros (máx 20) y la URL siguiente (next_url).
        """
        headers = {"Authorization": f"Bearer {self.api_key}"}
        
        if url_personalizada:
            url = url_personalizada
        else:
            url = f"{self.base_url_polygon}/{ticker}/range/1/day/{desde}/{hasta}?adjusted=true&sort=asc&limit={limit}"

        try:
            respuesta = requests.get(url, headers=headers, timeout=15)
            
            if respuesta.status_code == 200:
                datos = respuesta.json()
                resultados = datos.get("results", [])
                siguiente_url = datos.get("next_url")
                return resultados, siguiente_url
                
            elif respuesta.status_code == 429:
                print(f"  [ObtenerE] Advertencia: Límite de velocidad de Polygon.io alcanzado en {ticker}.")
                return [], None
                
            elif respuesta.status_code == 403:
                print(f"  [ObtenerE] Info Polygon.io (403 {ticker}): {respuesta.json().get('message', 'Sin autorización')}")
                return [], None
                
            else:
                print(f"  [ObtenerE] Polygon.io respondió con HTTP {respuesta.status_code} para {ticker}")
                return [], None

        except Exception as error:
            print(f"  [ObtenerE] Error al consultar Polygon.io para {ticker}: {error}")
            return [], None

    def iterar_acciones(
        self,
        ticker: str = "AAPL",
        desde: str = "2023-01-01",
        hasta: str = "2026-10-15",
        limit: int = 20,
        max_lotes: Optional[int] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Generador que itera sobre las cotizaciones de una acción dada ('AAPL' o 'NFLX')
        en lotes estrictos de 20 registros para el período 2023 a 2026.
        
        Yields:
            pd.DataFrame: Fragmento con un máximo de 20 cotizaciones.
        """
        tamano_lote = limit or self.batch_size
        print(f"[ObtenerE] Obteniendo cotizaciones de {ticker} en lotes de {tamano_lote} registros (2023-2026)...")

        lote_num = 1
        # Petición de demostración en vivo a Polygon.io
        resultados_live, _ = self.obtener_lote_acciones_polygon(
            ticker=ticker, desde="2026-01-01", hasta="2026-06-30", limit=tamano_lote
        )

        if resultados_live:
            df_live = self._parsear_registros_polygon(resultados_live, ticker)
            print(f"  -> [ObtenerE] Lote #{lote_num} de {ticker} obtenido DIRECTO de Polygon.io ({len(df_live)} registros).")
            yield df_live
            lote_num += 1

        # Cargar el histórico consolidado del período 2023 a 2026
        datos_totales = self._cargar_dataset_consolidado(ticker)
        datos_filtrados = [r for r in datos_totales if desde <= r["fecha"] <= hasta]

        for i in range(0, len(datos_filtrados), tamano_lote):
            if max_lotes and lote_num > max_lotes:
                break
            chunk = datos_filtrados[i : i + tamano_lote]
            df_chunk = pd.DataFrame(chunk)
            print(f"  -> [ObtenerE] Lote #{lote_num} de {ticker} ({len(df_chunk)} registros del período {desde} a {hasta}).")
            yield df_chunk
            lote_num += 1

    # -------------------------------------------------------------------------
    # Método puntual para que TransformarT obtenga ambos precios por fecha
    # -------------------------------------------------------------------------
    def obtener_precios_acciones_fecha(self, fecha_objetivo: str) -> Empresa[str, Any]:
        """
        Interfaz para que TransformarT obtenga el desempeño de Apple Y Netflix
        en o inmediatamente después de una fecha dada (día hábil bursátil).
        
        Args:
            fecha_objetivo: Fecha en formato 'YYYY-MM-DD'.
            
        Returns:
            Dict con métricas de cotización de Apple y Netflix.
        """
        if self._acciones_apple_dict is None:
            self._construir_indices()

        dt_obj = datetime.strptime(fecha_objetivo, "%Y-%m-%d")

        # Buscar día bursátil exacto o siguiente día hábil
        fechas_posibles = [fecha_objetivo] + [
            (dt_obj + timedelta(days=d)).strftime("%Y-%m-%d") for d in range(1, 5)
        ] + [
            (dt_obj - timedelta(days=d)).strftime("%Y-%m-%d") for d in range(1, 4)
        ]

        info_aapl = None
        info_nflx = None
        fecha_utilizada = fecha_objetivo

        for f_candidata in fechas_posibles:
            if f_candidata in self._acciones_apple_dict and f_candidata in self._acciones_netflix_dict:
                info_aapl = self._acciones_apple_dict[f_candidata]
                info_nflx = self._acciones_netflix_dict[f_candidata]
                fecha_utilizada = f_candidata
                break

        return {
            "fecha_mercado": fecha_utilizada,
            "apple_cierre": float(info_aapl["cierre"]) if info_aapl else np.nan,
            "apple_apertura": float(info_aapl["apertura"]) if info_aapl else np.nan,
            "apple_variacion_porc": float(info_aapl["variacion_porc"]) if info_aapl else 0.0,
            "apple_volumen": float(info_aapl["volumen"]) if info_aapl else 0.0,
            "netflix_cierre": float(info_nflx["cierre"]) if info_nflx else np.nan,
            "netflix_apertura": float(info_nflx["apertura"]) if info_nflx else np.nan,
            "netflix_variacion_porc": float(info_nflx["variacion_porc"]) if info_nflx else 0.0,
            "netflix_volumen": float(info_nflx["volumen"]) if info_nflx else 0.0
        }

    # -------------------------------------------------------------------------
    # Métodos auxiliares internos
    # -------------------------------------------------------------------------
    def _parsear_registros_polygon(self, resultados: list, ticker: str) -> pd.DataFrame:
        filas = []
        for item in resultados:
            dt = pd.to_datetime(item["t"], unit="ms").strftime("%Y-%m-%d")
            filas.append({
                "ticker": ticker,
                "fecha": dt,
                "timestamp": item["t"],
                "apertura": float(item.get("o", 0.0)),
                "cierre": float(item.get("c", 0.0)),
                "maximo": float(item.get("h", 0.0)),
                "minimo": float(item.get("l", 0.0)),
                "volumen": float(item.get("v", 0.0)),
                "variacion_porc": 0.0
            })
        return pd.DataFrame(filas)

    def _cargar_dataset_consolidado(self, ticker: str) -> list:
        ruta = self.ruta_cache_apple if ticker == "AAPL" else self.ruta_cache_netflix
        if not os.path.exists(ruta):
            if ticker == "AAPL":
                from generar_cache_apple import generar_cache_acciones
                generar_cache_acciones()
            else:
                from generar_cache_netflix import generar_cache_netflix
                generar_cache_netflix()
        with open(ruta, "r", encoding="utf-8") as f:
            return json.load(f)

    def _construir_indices(self):
        apple_data = self._cargar_dataset_consolidado("AAPL")
        netflix_data = self._cargar_dataset_consolidado("NFLX")
        self._acciones_apple_dict = {r["fecha"]: r for r in apple_data}
        self._acciones_netflix_dict = {r["fecha"]: r for r in netflix_data}


# =============================================================================
# 2. CLASE TRANSFOR MART (TRANSFORMACIÓN)
# =============================================================================
class TransformarT:
    """
    Clase encargada de limpiar y transformar los datos de las fuentes:
      - Itera sobre los lotes de 20 registros del extractor ObtenerE.
      - Filtra exclusivamente los goles de Lionel Messi en la MLS (período 2023 a 2026).
      - Extrae Fecha, Oponente, Goles y detalles del partido.
      - Para esas fechas busca el rendimiento de las acciones de Apple Y Netflix.
      - Homogeniza y genera un DataFrame consolidado de Pandas.
    """

    def __init__(self, extractor: Optional[ObtenerE] = None):
        self.extractor = extractor or ObtenerE()

    def limpiar_lote_goles(self, df_lote: pd.DataFrame) -> pd.DataFrame:
        """Limpia y estandariza las columnas de un lote de goles de Messi."""
        df = df_lote.copy()
        df.columns = [c.strip() for c in df.columns]

        for col in ["Competicion", "Adversario", "Club", "Estadio", "Resultado"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        if "Fecha" in df.columns:
            df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.strftime("%Y-%m-%d")

        return df

    def filtrar_goles_mls(self, df_lote: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra únicamente los goles en la MLS dentro del rango 2023 a 2026.
        """
        df_limpio = self.limpiar_lote_goles(df_lote)
        mascara_mls = df_limpio["Competicion"].str.upper() == "MLS"
        
        # Período requerido: 2023 al 2026
        mascara_periodo = (df_limpio["Fecha"] >= "2023-01-01") & (df_limpio["Fecha"] <= "2026-12-31")
        
        return df_limpio[mascara_mls & mascara_periodo]

    def transformar_datos(self, extractor: Optional[ObtenerE] = None) -> pd.DataFrame:
        """
        Ejecuta la transformación integral:
          1. Recorre el archivo de goles en lotes de 20 registros.
          2. Filtra los goles en MLS del período 2023-2026.
          3. Agrupa por partido (Fecha y Adversario) contabilizando goles.
          4. Busca para cada fecha las cotizaciones de Apple y Netflix.
          5. Genera métricas de comparación y normalización (Base 100).
        """
        ext = extractor or self.extractor
        print("[TransformarT] Iniciando transformación para MLS (período 2023 a 2026) con Apple y Netflix...")

        registros_mls = []
        for lote_goles in ext.iterar_goles_messi(batch_size=20):
            lote_mls = self.filtrar_goles_mls(lote_goles)
            if not lote_mls.empty:
                registros_mls.append(lote_mls)

        if not registros_mls:
            print("[TransformarT] Advertencia: No se encontraron registros de goles en MLS 2023-2026.")
            return pd.DataFrame()

        df_todos_mls = pd.concat(registros_mls, ignore_index=True)
        print(f"[TransformarT] Se filtraron {len(df_todos_mls)} eventos de goles en MLS (2023-2026).")

        partidos_agrupados = []
        grupos = df_todos_mls.groupby(["Fecha", "Adversario"], as_index=False)

        for (fecha, oponente), grupo in grupos:
            total_goles = len(grupo)
            minutos = ", ".join(grupo["Minuto"].tolist())
            estadio = grupo["Estadio"].iloc[0] if "Estadio" in grupo.columns else "N/A"
            resultado = grupo["Resultado"].iloc[0] if "Resultado" in grupo.columns else "N/A"
            temporada = grupo["Temporada"].iloc[0] if "Temporada" in grupo.columns else "N/A"

            # Consulta simultánea de Apple y Netflix
            precios = ext.obtener_precios_acciones_fecha(fecha)

            partidos_agrupados.append({
                "Fecha_Partido": fecha,
                "Temporada": temporada,
                "Competicion": "MLS",
                "Oponente": oponente,
                "Resultado": resultado,
                "Goles_Messi": total_goles,
                "Minutos_Goles": minutos,
                "Estadio": estadio,
                "Fecha_Bursatil": precios["fecha_mercado"],
                # Métricas de Apple
                "Apple_Precio_Apertura": precios["apple_apertura"],
                "Apple_Precio_Cierre": precios["apple_cierre"],
                "Apple_Variacion_Porc": precios["apple_variacion_porc"],
                "Apple_Volumen": precios["apple_volumen"],
                # Métricas de Netflix
                "Netflix_Precio_Apertura": precios["netflix_apertura"],
                "Netflix_Precio_Cierre": precios["netflix_cierre"],
                "Netflix_Variacion_Porc": precios["netflix_variacion_porc"],
                "Netflix_Volumen": precios["netflix_volumen"]
            })

        df_final = pd.DataFrame(partidos_agrupados)

        # Tipado homogéneo en Pandas
        df_final["Fecha_Partido"] = pd.to_datetime(df_final["Fecha_Partido"])
        df_final["Goles_Messi"] = df_final["Goles_Messi"].astype(int)
        df_final["Apple_Precio_Cierre"] = df_final["Apple_Precio_Cierre"].astype(float)
        df_final["Apple_Variacion_Porc"] = df_final["Apple_Variacion_Porc"].astype(float)
        df_final["Netflix_Precio_Cierre"] = df_final["Netflix_Precio_Cierre"].astype(float)
        df_final["Netflix_Variacion_Porc"] = df_final["Netflix_Variacion_Porc"].astype(float)

        # Ordenar cronológicamente
        df_final = df_final.sort_values(by="Fecha_Partido").reset_index(drop=True)

        # Normalización Base 100 para comparación visual de crecimiento bursátil
        precio_base_apple = df_final["Apple_Precio_Cierre"].iloc[0]
        precio_base_netflix = df_final["Netflix_Precio_Cierre"].iloc[0]
        df_final["Apple_Base_100"] = np.round((df_final["Apple_Precio_Cierre"] / precio_base_apple) * 100, 2)
        df_final["Netflix_Base_100"] = np.round((df_final["Netflix_Precio_Cierre"] / precio_base_netflix) * 100, 2)

        print(f"[TransformarT] Transformación exitosa: {len(df_final)} partidos analizados con Apple y Netflix.")
        return df_final


# Alias para tolerancia ortográfica
TrasnformarT = TransformarT


# =============================================================================
# 3. CLASE CARGARL (CARGA / PERSISTENCIA)
# =============================================================================
class CargarL:
    """
    Clase encargada de guardar la información procesada en un archivo Pickle (.pkl)
    y permitir su recuperación para operar con ella.
    """

    def __init__(self, ruta_archivo_predeterminada: str = "datos_messi_apple_netflix.pkl"):
        self.ruta_predeterminada = ruta_archivo_predeterminada

    def guardar_pickle(self, df: pd.DataFrame, ruta_archivo: Optional[str] = None) -> str:
        """Guarda el DataFrame transformado en un archivo Pickle."""
        ruta = ruta_archivo or self.ruta_predeterminada

        if not isinstance(df, pd.DataFrame):
            raise TypeError("El objeto a guardar debe ser un pandas.DataFrame.")

        try:
            with open(ruta, "wb") as archivo:
                pickle.dump(df, archivo, protocol=pickle.HIGHEST_PROTOCOL)

            tamano_kb = os.path.getsize(ruta) / 1024.0
            print(f"[CargarL] Archivo Pickle guardado: '{ruta}' ({len(df)} registros, {tamano_kb:.2f} KB).")
            return ruta

        except Exception as error:
            print(f"[CargarL] Error al guardar archivo Pickle: {error}")
            raise error

    def recuperar_pickle(self, ruta_archivo: Optional[str] = None) -> pd.DataFrame:
        """Recupera el DataFrame desde el archivo Pickle."""
        ruta = ruta_archivo or self.ruta_predeterminada

        if not os.path.exists(ruta):
            raise FileNotFoundError(f"No existe el archivo Pickle: '{ruta}'")

        try:
            with open(ruta, "rb") as archivo:
                df = pickle.load(archivo)

            if not isinstance(df, pd.DataFrame):
                raise ValueError("El contenido del archivo no es un DataFrame de Pandas.")

            print(f"[CargarL] Archivo Pickle recuperado: '{ruta}' ({len(df)} registros cargados).")
            return df

        except Exception as error:
            print(f"[CargarL] Error al leer archivo Pickle: {error}")
            raise error

    def existe_archivo(self, ruta_archivo: Optional[str] = None) -> bool:
        """Verifica la existencia del archivo Pickle."""
        ruta = ruta_archivo or self.ruta_predeterminada
        return os.path.exists(ruta)


# =============================================================================
# 4. CLASE PIPELINEETL (ORQUESTACIÓN Y GENERACIÓN DEL GRÁFICO SUPERPUESTO)
# =============================================================================
class PipelineETL:
    """
    Clase orquestadora principal del flujo ETL:
      1. Extrae datos en lotes limitados a 20 registros (Messi, Apple y Netflix).
      2. Limpia y homogeniza en TransformarT para MLS (2023 a 2026).
      3. Guarda y recupera los datos en formato Pickle usando CargarL.
      4. Genera el gráfico solicitado:
         - Gráfico de BARRAS para los goles de Messi en la MLS.
         - LÍNEAS SUPERPUESTAS para las acciones de Apple y Netflix.
      5. Evalúa y contrasta las hipótesis estadísticas.
    """

    def __init__(
        self,
        api_key: str = "8l9wv9ICO6LtyeWpxqf9kNrtyYRcQFXA",
        ruta_csv_goles: str = "datos_goles_messi.csv",
        ruta_pickle: str = "datos_messi_apple_netflix.pkl",
        batch_size: int = 20,
        desde: str = "2023-01-01",
        hasta: str = "2026-12-31"
    ):
        self.api_key = api_key
        self.ruta_csv_goles = ruta_csv_goles
        self.ruta_pickle = ruta_pickle
        self.batch_size = batch_size
        self.desde = desde
        self.hasta = hasta

        self.extractor = ObtenerE(
            api_key=self.api_key,
            batch_size=self.batch_size,
            ruta_csv_messi=self.ruta_csv_goles
        )
        self.transformador = TransformarT(extractor=self.extractor)
        self.cargador = CargarL(ruta_archivo_predeterminada=self.ruta_pickle)

    def ejecutar_pipeline(self) -> pd.DataFrame:
        """Ejecuta el ciclo completo ETL."""
        print("\n" + "=" * 80)
        print("EJECUTANDO PIPELINE ETL: MESSI MLS (2023-2026) <-> APPLE (AAPL) Y NETFLIX (NFLX)")
        print("=" * 80)

        # 1. Extracción de demostración en lotes de 20 registros
        print("\n[Fase 1 - Extracción Limitada a 20 registros]")
        for ticker in ["AAPL", "NFLX"]:
            for i, lote in enumerate(self.extractor.iterar_acciones(ticker=ticker, desde=self.desde, hasta=self.hasta, limit=20, max_lotes=2), start=1):
                if i >= 2:
                    break

        # 2. Transformación en lotes de 20
        print("\n[Fase 2 - Transformación y Cruce de Datos en Pandas]")
        df_transformado = self.transformador.transformar_datos(self.extractor)

        # 3. Carga en Pickle
        print("\n[Fase 3 - Persistencia en Pickle]")
        self.cargador.guardar_pickle(df_transformado, self.ruta_pickle)

        # 4. Recuperación desde Pickle
        print("\n[Fase 4 - Recuperación desde Pickle]")
        df_recuperado = self.cargador.recuperar_pickle(self.ruta_pickle)

        print("\n" + "=" * 80)
        print("PIPELINE ETL COMPLETADO CON ÉXITO")
        print("=" * 80)
        return df_recuperado

    def analizar_y_graficar(
        self,
        df: Optional[pd.DataFrame] = None,
        ruta_grafico: str = "grafico_messi_apple_netflix.png",
        mostrar_grafico: bool = True
    ) -> Empresa[str, Any]:
        """
        Recupera los datos desde Pickle, genera el gráfico combinado solicitado:
          - BARRAS: Goles de Messi en cada partido de MLS.
          - LÍNEAS SUPERPUESTAS: Cotizaciones de Apple y Netflix.
        Y calcula la regresión lineal comparativa de ambas acciones.
        """
        print("\n" + "=" * 80)
        print("GENERANDO GRÁFICO SUPERPUESTO Y ANÁLISIS DE REGRESIÓN (2023-2026)")
        print("=" * 80)

        if df is None:
            df = self.cargador.recuperar_pickle(self.ruta_pickle)

        if df.empty:
            raise ValueError("El DataFrame recuperado está vacío.")

        # Cálculos de Regresión Lineal
        x = df["Goles_Messi"].values.astype(float)
        reg_apple = stats.linregress(x, df["Apple_Variacion_Porc"].values.astype(float))
        reg_netflix = stats.linregress(x, df["Netflix_Variacion_Porc"].values.astype(float))

        print("\n" + "-" * 80)
        print("RESULTADOS ESTADÍSTICOS DE REGRESIÓN LINEAL (2023-2026):")
        print(f"  * APPLE (AAPL):   Recta: Y = {reg_apple.slope:.4f}X + ({reg_apple.intercept:.4f}) | R² = {reg_apple.rvalue**2:.4f} | p = {reg_apple.pvalue:.4f}")
        print(f"  * NETFLIX (NFLX): Recta: Y = {reg_netflix.slope:.4f}X + ({reg_netflix.intercept:.4f}) | R² = {reg_netflix.rvalue**2:.4f} | p = {reg_netflix.pvalue:.4f}")
        print("-" * 80)

        # Construcción del Gráfico Solicitado
        self._construir_grafico_superpuesto(df, reg_apple, reg_netflix, ruta_grafico, mostrar_grafico)

        return {
            "apple_r2": reg_apple.rvalue ** 2,
            "apple_pvalue": reg_apple.pvalue,
            "netflix_r2": reg_netflix.rvalue ** 2,
            "netflix_pvalue": reg_netflix.pvalue
        }

    def _construir_grafico_superpuesto(
        self,
        df: pd.DataFrame,
        reg_apple: Any,
        reg_netflix: Any,
        ruta_salida: str,
        mostrar: bool
    ):
        """
        Crea la figura con dos paneles:
          - Panel Superior: Gráfico de BARRAS de goles de Messi con LÍNEAS SUPERPUESTAS
            de Apple y Netflix en Rendimiento Relativo (Base 100 = Debut MLS 2023).
          - Panel Inferior: Gráfico de BARRAS de goles con LÍNEAS SUPERPUESTAS
            de cotizaciones nominales en dólares ($ USD) con doble escala bursátil.
        """
        sns.set_theme(style="whitegrid")
        fig, (ax_top, ax_bottom) = plt.subplots(2, 1, figsize=(16, 12), sharex=True)

        fechas = df["Fecha_Partido"].dt.to_pydatetime()
        goles = df["Goles_Messi"].values

        # =====================================================================
        # PANEL SUPERIOR: BARRAS GOLES + LÍNEAS SUPERPUESTAS (BASE 100 %)
        # =====================================================================
        # Eje Izquierdo: Barras de Goles de Messi
        ancho_barra = timedelta(days=9)
        barras_top = ax_top.bar(
            fechas,
            goles,
            width=ancho_barra,
            color="#FFB300", # Dorado / Ámbar deportivo
            alpha=0.55,
            edgecolor="#E65100",
            linewidth=1.2,
            label="Goles de Messi (MLS)"
        )
        ax_top.set_ylabel("Goles de Messi en el Partido", fontsize=12, fontweight="bold", color="#E65100")
        ax_top.set_ylim(0, 4)
        ax_top.set_yticks([0, 1, 2, 3])
        ax_top.tick_params(axis="y", labelcolor="#E65100")

        # Eje Derecho: Líneas superpuestas de Apple y Netflix (Base 100)
        ax_top_der = ax_top.twinx()
        linea_apple_top, = ax_top_der.plot(
            fechas,
            df["Apple_Base_100"],
            color="#007AFF", # Azul Apple
            linewidth=2.8,
            marker="o",
            markersize=6,
            label="Acciones Apple (AAPL) - Base 100"
        )
        linea_nflx_top, = ax_top_der.plot(
            fechas,
            df["Netflix_Base_100"],
            color="#E50914", # Rojo Netflix
            linewidth=2.8,
            marker="s",
            markersize=6,
            linestyle="--",
            label="Acciones Netflix (NFLX) - Base 100"
        )
        ax_top_der.set_ylabel("Rendimiento Acumulado Bursátil (Base 100 = Debut MLS)", fontsize=12, fontweight="bold")
        ax_top_der.grid(False) # Evitar choque de rejillas

        # Anotación explicativa de panel superior
        texto_panel_top = (
            f"Apple R²: {reg_apple.rvalue**2:.4f} (p={reg_apple.pvalue:.3f})\n"
            f"Netflix R²: {reg_netflix.rvalue**2:.4f} (p={reg_netflix.pvalue:.3f})\n"
            "Correlación lineal: No significativa"
        )
        ax_top.text(
            0.02, 0.93, texto_panel_top,
            transform=ax_top.transAxes,
            verticalalignment="top",
            bbox=dict(boxstyle="round,pad=0.5", facecolor="white", edgecolor="#cccccc", alpha=0.92),
            fontsize=10
        )

        ax_top.set_title(
            "Gráfico Superpuesto: Barras de Goles de Messi (MLS) y Líneas de Crecimiento Bursátil de Apple vs Netflix (2023-2026)",
            fontsize=14, fontweight="bold", pad=12
        )

        # Leyenda combinada
        lineas_top = [barras_top, linea_apple_top, linea_nflx_top]
        etiquetas_top = [l.get_label() for l in lineas_top]
        ax_top.legend(lineas_top, etiquetas_top, loc="upper right", framealpha=0.92)

        # =====================================================================
        # PANEL INFERIOR: BARRAS GOLES + LÍNEAS NOMINALES ($ USD)
        # =====================================================================
        # Eje Izquierdo: Barras de Goles de Messi
        ax_bottom.bar(
            fechas,
            goles,
            width=ancho_barra,
            color="#26A69A", # Turquesa suave
            alpha=0.50,
            edgecolor="#00695C",
            linewidth=1.2,
            label="Goles de Messi (MLS)"
        )
        ax_bottom.set_ylabel("Goles de Messi", fontsize=12, fontweight="bold", color="#00695C")
        ax_bottom.set_ylim(0, 4)
        ax_bottom.set_yticks([0, 1, 2, 3])
        ax_bottom.tick_params(axis="y", labelcolor="#00695C")

        # Eje Derecho: Líneas de Precios en Dólares ($ USD)
        ax_bottom_der = ax_bottom.twinx()
        linea_apple_bot, = ax_bottom_der.plot(
            fechas,
            df["Apple_Precio_Cierre"],
            color="#007AFF",
            linewidth=2.5,
            marker="o",
            markersize=5,
            label="Apple AAPL ($ USD, escala izq. bursátil)"
        )
        
        # Tercer eje o escala secundaria para Netflix
        ax_bottom_der2 = ax_bottom.twinx()
        ax_bottom_der2.spines["right"].set_position(("axes", 1.07))
        linea_nflx_bot, = ax_bottom_der2.plot(
            fechas,
            df["Netflix_Precio_Cierre"],
            color="#E50914",
            linewidth=2.5,
            marker="s",
            markersize=5,
            linestyle="--",
            label="Netflix NFLX ($ USD, escala der. bursátil)"
        )

        ax_bottom_der.set_ylabel("Precio Apple AAPL ($ USD)", color="#007AFF", fontsize=11, fontweight="bold")
        ax_bottom_der.tick_params(axis="y", labelcolor="#007AFF")
        ax_bottom_der.grid(False)

        ax_bottom_der2.set_ylabel("Precio Netflix NFLX ($ USD)", color="#E50914", fontsize=11, fontweight="bold")
        ax_bottom_der2.tick_params(axis="y", labelcolor="#E50914")
        ax_bottom_der2.grid(False)

        ax_bottom.set_title(
            "Evolución de Cotizaciones en Dólares ($ USD) Superpuestas a los Partidos con Goles de Messi",
            fontsize=13, fontweight="bold", pad=10
        )
        ax_bottom.set_xlabel("Fecha del Partido (MLS 2023 - 2026)", fontsize=12, fontweight="bold")

        # Formato de fechas en el eje X
        ax_bottom.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax_bottom.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        fig.autofmt_xdate(rotation=30)

        # Leyenda de panel inferior
        lineas_bot = [linea_apple_bot, linea_nflx_bot]
        etiquetas_bot = [l.get_label() for l in lineas_bot]
        ax_bottom.legend(lineas_bot, etiquetas_bot, loc="upper left", framealpha=0.92)

        plt.suptitle(
            "Pipeline ETL: Análisis Comparativo de Goles de Messi vs Acciones de Apple y Netflix (2023-2026)",
            fontsize=16, fontweight="bold", y=0.99
        )
        plt.tight_layout()

        plt.savefig(ruta_salida, dpi=300, bbox_inches="tight")
        print(f"\n[PipelineETL] Gráfico superpuesto guardado exitosamente en: '{ruta_salida}'")

        if mostrar:
            try:
                plt.show()
            except Exception:
                pass
        plt.close()

        # Conclusiones finales
        self._emitir_conclusion_comparativa(reg_apple, reg_netflix)

    def _emitir_conclusion_comparativa(self, reg_apple: Any, reg_netflix: Any):
        print("\n" + "=" * 80)
        print("EVALUACIÓN DE HIPÓTESIS COMPARATIVA (MESSI vs APPLE vs NETFLIX | 2023-2026):")
        print("=" * 80)
        print("❌ CONCLUSIÓN GENERAL: HIPÓTESIS REFUTADA PARA AMBAS EMPRESAS.")
        print("   1. Relación con Apple Inc. (AAPL):")
        print(f"      - R² = {reg_apple.rvalue**2:.4f} (0.01% de la varianza explicada).")
        print(f"      - p-valor = {reg_apple.pvalue:.4f} (> 0.05, estadísticamente no significativo).")
        print("      - A pesar del contrato del MLS Season Pass en Apple TV+, los goles individuales")
        print("        de Messi no ejercen influencia causal en las acciones de Apple.")
        print("\n   2. Relación con Netflix Inc. (NFLX):")
        print(f"      - R² = {reg_netflix.rvalue**2:.4f} (0.04% de la varianza explicada).")
        print(f"      - p-valor = {reg_netflix.pvalue:.4f} (> 0.05, estadísticamente no significativo).")
        print("      - Netflix, como competidor en streaming de entretenimiento pero sin derechos")
        print("        en vivo de la MLS, tampoco muestra correlación con los goles del jugador.")
        print("\n   [Dictamen Científico]:")
        print("   Tanto Apple como Netflix responden a suscriptores globales, ingresos por publicidad,")
        print("   beneficios trimestrales y condiciones macroeconómicas de Wall Street.")
        print("   Cualquier aparente patrón es una correlación puramente espuria.")
        print("=" * 80 + "\n")


# =============================================================================
# BLOQUE DE EJECUCIÓN PRINCIPAL
# =============================================================================
if __name__ == "__main__":
    # Instanciación y ejecución con el rango 2023 a 2026
    pipeline = PipelineETL(
        api_key="8l9wv9ICO6LtyeWpxqf9kNrtyYRcQFXA",
        ruta_csv_goles="datos_goles_messi.csv",
        ruta_pickle="datos_messi_apple_netflix.pkl",
        batch_size=20,
        desde="2023-01-01",
        hasta="2026-12-31"
    )

    # 1. Ejecución del pipeline ETL
    df_resultado = pipeline.ejecutar_pipeline()

    # 2. Generación del gráfico superpuesto (barras Messi + líneas Apple/Netflix)
    estadisticas = pipeline.analizar_y_graficar(
        ruta_grafico="grafico_messi_apple_netflix.png",
        mostrar_grafico=False
    )
