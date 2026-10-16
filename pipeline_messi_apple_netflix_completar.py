"""
=============================================================================
PIPELINE ETL: RELACIÓN ENTRE GOLES DE MESSI, ACCIONES DE APPLE (AAPL) Y NETFLIX (NFLX)
=============================================================================

Consideraciones de diseño de esta versión:
  1. Nombres de clases actualizados:
     - E_Obtener: Clase de extracción y gestión de archivos intermedios.
     - T_Transformar: Clase de homogenización, transformación y relleno de faltantes.
     - L_Cargar: Clase de persistencia y carga del dataset final en Pickle.
     - PipelineETL: Clase orquestadora y generadora del gráfico conjunto.
  2. Filtro en 'extraer_goles_messi_intermedio':
     - Filtra directamente en el método de extracción del CSV exclusivamente
       las fechas de los goles de Lionel Messi en la MLS.
  3. Métodos de guardar y obtener archivos intermedios en E_Obtener:
     - 'guardar_intermedio' y 'obtener_intermedio' pertenecen a E_Obtener.
     - Administra tickers y rutas mediante el diccionario de instancia 'self.activos'.
     - Los métodos de obtención reciben un elemento de ese diccionario sin validar strings.
  4. Relleno de datos faltantes con aproximaciones lineales en T_Transformar:
     - 'rellenar_datos_faltantes_messi'
     - 'rellenar_datos_faltantes_apple'
     - 'rellenar_datos_faltantes_netflix'
     - Los rellenos de valores bursátiles para Netflix y Apple se realizan
       mediante APROXIMACIONES LINEALES (interpolate lineal) entre los días que
       tienen valor sobre los valores NaN del dataframe obtenido en E_Obtener.
  5. Agrupación por fechas sumando goles (groupby) y valores de CIERRE para el gráfico:
     - Gráfico de BARRAS para goles de Messi (suma por fecha).
     - LÍNEAS SUPERPUESTAS con los valores de CIERRE de Apple y Netflix ($ USD).

Período evaluado: 2023 a 2026.
"""

from _winapi import FILE_TYPE_CHAR
from socket import AI_PASSIVE
import os
import pickle
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

# Iterador oficial de Polygon.io
from polygon import RESTClient


# =============================================================================
# FUNCIÓN AUXILIAR DE OPTIMIZACIÓN DE MEMORIA RAM
# =============================================================================
def optimizar_enteros_ram(df: pd.DataFrame) -> pd.DataFrame:
    """
    Homogeniza las columnas de tipo número entero al menor subtipo posible
    (int8, int16, int32, int64 o unsigned) para minimizar el consumo de memoria RAM.
    """
    df_opt = df.copy()
    for col in df_opt.columns:
        if pd.api.types.is_integer_dtype(df_opt[col]):
            df_opt[col] = pd.to_numeric(df_opt[col], downcast="integer")
        elif pd.api.types.is_float_dtype(df_opt[col]):
            if not df_opt[col].isna().any():
                if (df_opt[col] % 1 == 0).all():
                    df_opt[col] = pd.to_numeric(df_opt[col], downcast="integer")
    return df_opt


# =============================================================================
# 1. CLASE E_OBTENER (EXTRACCIÓN Y ARCHIVOS INTERMEDIOS)
# =============================================================================
class E_Obtener:
    """
    Clase de extracción de datos:
      - Encargada de obtener los datos dentro de los rangos desde las fuentes
        (API Polygon.io para Apple y Netflix; archivo CSV para Messi).
      - El método 'extraer_goles_messi_intermedio' filtra exclusivamente las fechas
        de los goles de Messi en la MLS.
      - Administra rutas y tickers mediante el diccionario de instancia 'self.activos'.
      - El método de obtención de Polygon recibe un elemento de ese diccionario.
      - Métodos 'guardar_intermedio' y 'obtener_intermedio' para persistir
        los archivos intermedios de Messi, Apple y Netflix.
    """

    def __init__(self):
        # 1. Variables de instancia operativas
        self.fecha_inicio: str = "2023-01-01"
        self.fecha_fin: str = "2026-12-31"
        self.api_key: str = "CLAVE DE API AQUI"
        self.url_base_polygon: str = "https://api.polygon.io/v2/aggs/ticker"

        # 2. Diccionario de instancia con los tickers y sus carpetas asociadas
        self.activos: Dict[str, Dict[str, str]] = {
            "AAPL": {
                "ticker": "AAPL",
                "empresa": "Apple Inc.",
                "carpeta": os.path.join("datos", "apple"),
                "ruta_intermedia": os.path.join("datos", "apple", "acciones_apple_intermedio.pkl")
            },
            "NFLX": {
                "ticker": "NFLX",
                "empresa": "Netflix Inc.",
                "carpeta": os.path.join("datos", "netflix"),
                "ruta_intermedia": os.path.join("datos", "netflix", "acciones_netflix_intermedio.pkl")
            }
        }

        # Configuración de rutas para datos de Messi
        self.info_messi: Dict[str, str] = {
            "carpeta": os.path.join("datos", "messi"),
            "ruta_csv_origen": os.path.join("datos", "messi", "datos_goles_messi.csv"),
            "ruta_intermedia": os.path.join("datos", "messi", "goles_messi_intermedio.pkl")
        }

        # Cliente Polygon.io
        self.cliente_polygon = "DATOS PARA CREAR EL CLIENTE REST API AQUI"

    # -------------------------------------------------------------------------
    # Métodos de Guardar y Obtener Intermedio (Pertenecen a E_Obtener)
    # -------------------------------------------------------------------------
    def guardar_intermedio(self, df: pd.DataFrame, ruta_archivo: str) -> str:
        """
        Guarda un DataFrame intermedio en formato Pickle (.pkl),
        creando la carpeta de destino si no existe y optimizando enteros.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("El objeto intermedio a guardar debe ser un pandas.DataFrame.")

        carpeta = os.path.dirname(ruta_archivo)
        if carpeta:
            os.makedirs(carpeta, exist_ok=True)

        df_opt = optimizar_enteros_ram(df)
        ABRIR ruta_archivo
        GUADRAR DATAFRAME CON PICKLE

        tamano_kb = os.path.getsize(ruta_archivo) / 1024.0
        print(f"  -> [E_Obtener] Archivo intermedio guardado: '{ruta_archivo}' ({len(df_opt)} registros, {tamano_kb:.2f} KB).")
        return ruta_archivo

    def obtener_intermedio(self, ruta_archivo: str) -> pd.DataFrame:
        """
        Recupera un DataFrame intermedio previamente serializado en Pickle.
        """
        if not os.path.exists(ruta_archivo):
            raise FileNotFoundError(f"No existe el archivo intermedio en: '{ruta_archivo}'")

        ABRIR ruta_archivo
        RECUPERAR OBJETO CON PICKLE

        print(f"  -> [E_Obtener] Archivo intermedio leído: '{ruta_archivo}' ({len(df)} registros cargados).")
        return df

    # -------------------------------------------------------------------------
    # Extracción de Datos desde Polygon.io (Recibe un elemento del diccionario)
    # -------------------------------------------------------------------------
    def obtener_datos_polygon(self, empresa: Dict[str, str]) -> pd.DataFrame:
        """
        Obtiene los datos desde la API de Polygon.io utilizando su iterador nativo.
        Recibe como parámetro un elemento del diccionario 'self.activos',
        por lo que no se requiere validar ningún string ingresado.
        """
        ticker = OBTENER TIKER
        ruta_intermedia = OBTENER RUTA INTERMEDIA
        print(f"[E_Obtener] Extrayendo desde fuente Polygon.io para activo '{ticker}' (carpeta: '{empresa['carpeta']}')...")

        registros = []
        try:
            # Iterador nativo ofrecido por la API de Polygon.io
            OBTENER EL ITERADOR DE LA API 
            PASANDO LOS PARÁMETROS DE LA CONSULTA 

            
            for agg in aggs_iter:
                try:
                    CONVERTIR LAS FECHAS DEVUELTAS A TIPO FECHA 
                    CONVERTIR A FLOAT LOS VALORES DE VALOR DE CIERRE DE LA ACCIÓN EN ESE DÍA
                    GUADRAR LOS VALORES EN UNA LISTA DE REGISTROS
                    
                except StopIteration:
                    break

            if registros:
                print(f"  -> [E_Obtener] {len(registros)} registros extraídos directamente de Polygon.io para {ticker}.")

        except Exception as err:
            print(f"  [E_Obtener] Aviso sobre API Polygon.io ({ticker}): {err}")
        
        df_empresa = pd.DataFrame(registros)

        if(df_empresa.empty):
            raise ValueError(f"No se encontraron datos de {ticker} en el período especificado {self.fecha_inicio} - {self.fecha_fin}.")

        # Guardar en el archivo intermedio de esta fuente
        self.guardar_intermedio(df_empresa, ruta_intermedia)
        return df_empresa

    # -------------------------------------------------------------------------
    # Extracción Exclusiva de Fechas de Goles de Messi en MLS (Filtro Requerido)
    # -------------------------------------------------------------------------
    def extraer_goles_messi_intermedio(self) -> pd.DataFrame:
        """
        Lee el archivo CSV de goles de Messi iterando en bloques de self.batch_size
        mediante la variable interna de instancia,
        filtrando EXCLUSIVAMENTE los goles de Messi en la MLS en el período evaluado,
        y extrayendo las fechas correspondientes para el archivo intermedio.
        """
        ruta_csv = self.info_messi["ruta_csv_origen"]
        ruta_intermedia = self.info_messi["ruta_intermedia"]
        print(f"[E_Obtener] Extrayendo del CSV exclusivamente las fechas de goles de Messi en la MLS (bloques de {self.batch_size})...")

        COMPROBAR SI EL ARCHIVO ESISTE, SINO LANZAR EXEPCION FILENOTFOUNDERROR

        LEER EL ARCHIVO ORIGEN CSV Y QUEDARSE CON LOS DATOS DE FECHA Y COMPETICION. 
        CONVERTIR LAS FECHAS A FORMATO FECHA PARA PODER USAR UNA MÁSCARA
        CREAR FILTRO POR LAS FECHAS DEFINIDAS EN EL OBJETIVO DEL PIPELINE Y POR LA COMPETICIÓN "MLS"

        # FILTRO SOLICITADO: Solo goles en MLS y sus fechas dentro del rango 2023-2026
    
        print(f"[E_Obtener] Filtro MLS aplicado en la extracción: {len(bloque_mls)} registros de goles en MLS obtenidos.")

        
        # Guardar en archivo intermedio de Messi
        self.guardar_intermedio(bloque_mls, ruta_intermedia)
        return bloque_mls


# =============================================================================
# 2. CLASE T_TRANSFORMAR (TRANSFORMACIÓN Y APROXIMACIONES LINEALES)
# =============================================================================
class T_Transformar:
    """
    Clase encargada de la homogenización, transformación y de rellenar datos faltantes:
      - Contiene un método específico para rellenar datos faltantes para CADA fuente:
          * rellenar_datos_faltantes_messi()
          * rellenar_datos_faltantes_apple()
          * rellenar_datos_faltantes_netflix()
      - El relleno de Apple y Netflix se realiza mediante APROXIMACIONES LINEALES
        (interpolate lineal) entre los días que tienen valor sobre los valores NaN
        del dataframe obtenido en E_Obtener.
      - Aplica el groupby de Pandas sobre fechas sumando goles.
      - Homogeniza los tipos de números enteros al menor posible (int8, int32) para ahorrar RAM.
    """

    def __init__(self, extractor: Optional[E_Obtener] = None):
        self.extractor = extractor or E_Obtener()
        self.fecha_inicio: str = self.extractor.fecha_inicio
        self.fecha_fin: str = self.extractor.fecha_fin

    # -------------------------------------------------------------------------
    # Método 1: Rellenar datos faltantes para MESSI
    # -------------------------------------------------------------------------
    def rellenar_datos_faltantes_messi(self, df_messi_intermedio: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia, valida fechas y aplica groupby sobre fechas sumando los goles
        de Messi para cada encuentro de la MLS.
        """
        print("[T_Transformar] Ejecutando 'rellenar_datos_faltantes_messi'...")
        df = df_messi_intermedio.copy()

        # Cada fila extraída representa 1 gol anotado en MLS
        df["Goles"] = 1

        # =====================================================================
        # GROUPBY DE PANDAS SOBRE FECHAS SUMANDO GOLES
        # =====================================================================
        HACER EL GOUPBY EN EL DATAFRAME DE PANDA

        # Homogenizar número entero de goles al menor tipo (int8)
        df_goles_sumados["Goles_Messi"] = pd.to_numeric(df_goles_sumados["Goles_Messi"], downcast="integer")
        print(f"  -> [T_Transformar] Messi: {len(df_goles_sumados)} fechas únicas consolidadas con goles sumados.")
        return df_goles_sumados

    # -------------------------------------------------------------------------
    # Método 2: Rellenar datos faltantes para APPLE (APROXIMACIÓN LINEAL)
    # -------------------------------------------------------------------------
    def rellenar_datos_faltantes_apple(self, df_apple_obtenido: pd.DataFrame, df_goles_sumados: pd.DataFrame) -> pd.DataFrame:
        """
        Rellena los valores bursátiles de Apple mediante APROXIMACIONES LINEALES
        entre los días que tienen valor sobre los valores NaN del dataframe
        con los datos obtenidos en E_Obtener.
        """
        print("[T_Transformar] Ejecutando 'rellenar_datos_faltantes_apple' con aproximación lineal sobre NaNs...")

        if ("Fecha" in df_goles_sumados):
            df_goles_sumados.rename(columns={"Fecha": "fecha"}, inplace=True)
        
        if(df_apple_obtenido.empty):
            raise ValueError("No se encontraron datos de Apple en el período especificado.")

        # Combinar calendario con los datos obtenidos en E_Obtener (los días sin cotización serán NaN)
        df_con_nans = COMBINAR LOS DATOS DE LAS ACCIONES DE APPLE CON LOS GOLES DE MESSI

        # =====================================================================
        # APROXIMACIÓN LINEAL ENTRE DÍAS CON VALOR SOBRE LOS VALORES NaN
        # =====================================================================
        df_con_nans["cierre"] = INTERPOLACION LINEAL DE LOS VALORES CON NAN DE LAS ACCIONES DE APPLE
        df_con_nans["cierre"] = REDONDEO DE LOS VALIRES DE LAS ACCIONES A 2 DIGITOS
        
        # Homogenizar enteros para minimizar consumo de RAM
        df_resultado = optimizar_enteros_ram(df_con_nans)
        print(f"  -> [T_Transformar] Apple: {len(df_resultado)} días completados por aproximación lineal.")
        return df_resultado

    # -------------------------------------------------------------------------
    # Método 3: Rellenar datos faltantes para NETFLIX (APROXIMACIÓN LINEAL)
    # -------------------------------------------------------------------------
    def rellenar_datos_faltantes_netflix(self, df_netflix_obtenido: pd.DataFrame, df_goles_sumados: pd.DataFrame) -> pd.DataFrame:
        """
        Rellena los valores bursátiles de Netflix mediante APROXIMACIONES LINEALES
        entre los días que tienen valor sobre los valores NaN del dataframe
        con los datos obtenidos en E_Obtener.
        """
        print("[T_Transformar] Ejecutando 'rellenar_datos_faltantes_netflix' con aproximación lineal sobre NaNs...")

        if ("Fecha" in df_goles_sumados):
            df_goles_sumados.rename(columns={"Fecha": "fecha"}, inplace=True)
        
        # Combinar calendario con los datos obtenidos en E_Obtener
        if(df_netflix_obtenido.empty):
            raise ValueError("No se encontraron datos de Netflix en el período especificado.")
        
        df_con_nans = COMBINAR LOS DATOS DE LAS ACCIONES DE NETFLIX CON LOS GOLES DE MESSI
        
        # =====================================================================
        # APROXIMACIÓN LINEAL ENTRE DÍAS CON VALOR SOBRE LOS VALORES NaN
        # =====================================================================
        df_con_nans["cierre"] = INTERPOLACION LINEAL DE LOS VALORES CON NAN DE LAS ACCIONES DE APPLE
        df_con_nans["cierre"] = REDONDEO DE LOS VALIRES DE LAS ACCIONES A 2 DIGITOS
        
        df_resultado = optimizar_enteros_ram(df_con_nans)
        print(f"  -> [T_Transformar] Netflix: {len(df_resultado)} días completados por aproximación lineal.")
        return df_resultado

    # -------------------------------------------------------------------------
    # Proceso de Transformación Integral y Cruce con Valores de Cierre
    # -------------------------------------------------------------------------
    def transformar_datos(
        self,
        df_messi_intermedio: pd.DataFrame,
        df_apple_intermedio: pd.DataFrame,
        df_netflix_intermedio: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Ejecuta el relleno de datos faltantes por fuente con aproximaciones lineales,
        cruza los goles sumados por fecha con los valores de CIERRE de las acciones
        y produce el DataFrame final consolidado.
        """
        print("\n[T_Transformar] Iniciando homogenización, aproximaciones lineales y cruce...")

        # 1. Relleno por fuente con aproximaciones lineales
        df_goles = self.rellenar_datos_faltantes_messi(df_messi_intermedio)
        df_apple = self.rellenar_datos_faltantes_apple(df_apple_intermedio, df_goles)
        df_netflix = self.rellenar_datos_faltantes_netflix(df_netflix_intermedio, df_goles)
        
        dict_aapl = df_apple.set_index("fecha").to_dict(orient="index")
        dict_nflx = df_netflix.set_index("fecha").to_dict(orient="index")

        filas_cruce = []
        for _, fila in df_goles.iterrows():
            fecha_partido = fila["fecha"]
            dt_obj = datetime.strptime(fecha_partido, "%Y-%m-%d")

            # Buscar fecha bursátil más cercana
            fechas_candidatas = [fecha_partido] + [
                (dt_obj + timedelta(days=d)).strftime("%Y-%m-%d") for d in range(1, 5)
            ] + [
                (dt_obj - timedelta(days=d)).strftime("%Y-%m-%d") for d in range(1, 4)
            ]

            info_aapl = None
            info_nflx = None
            fecha_bursatil = fecha_partido

            for fc in fechas_candidatas:
                if fc in dict_aapl and fc in dict_nflx:
                    info_aapl = dict_aapl[fc]
                    info_nflx = dict_nflx[fc]
                    fecha_bursatil = fc
                    break

            filas_cruce.append({
                "Fecha_Partido": fecha_partido,
                "Fecha_Bursatil": fecha_bursatil,
                "Goles_Messi": fila["Goles_Messi"],
                # VALORES DE CIERRE DE LAS ACCIONES
                "Apple_Precio_Cierre": float(info_aapl["cierre"]) if info_aapl else np.nan,
                "Netflix_Precio_Cierre": float(info_nflx["cierre"]) if info_nflx else np.nan
            })

        df_final = pd.DataFrame(filas_cruce)
        df_final["Fecha_Partido"] = pd.to_datetime(df_final["Fecha_Partido"])
        df_final = df_final.sort_values(by="Fecha_Partido").reset_index(drop=True)

        # Optimización final de memoria RAM en enteros
        df_final = optimizar_enteros_ram(df_final)

        print(f"[T_Transformar] Transformación finalizada: {len(df_final)} partidos vinculados a valores de cierre de Apple y Netflix.")
        return df_final


# =============================================================================
# 3. CLASE L_CARGAR (PERSISTENCIA Y RECUPERACIÓN DEL ARCHIVO FINAL)
# =============================================================================
class L_Cargar:
    """
    Clase encargada de guardar el archivo consolidado final en formato Pickle (.pkl)
    y de recuperarlo para el análisis y graficación.
    """

    def __init__(self, ruta_archivo_final: str = "datos/datos_messi_apple_netflix.pkl"):
        self.ruta_archivo_final: str = ruta_archivo_final

    def guardar_datos_finales(self, df: pd.DataFrame) -> str:
        """Serializa el dataset final en Pickle optimizando enteros."""
        if not isinstance(df, pd.DataFrame):
            raise TypeError("El objeto final debe ser un pandas.DataFrame.")

        df_opt = optimizar_enteros_ram(df)
        ABRIR ruta_archivo_final Y GUARDAR CON PICKLE LOS DATOS CONSOLIDADOS
            
        tamano_kb = os.path.getsize(self.ruta_archivo_final) / 1024.0
        print(f"[L_Cargar] Dataset final guardado en Pickle: '{self.ruta_archivo_final}' ({len(df_opt)} filas, {tamano_kb:.2f} KB).")
        return self.ruta_archivo_final

    def cargar_datos_finales(self) -> pd.DataFrame:
        """Carga el dataset final desde Pickle."""
        if not os.path.exists(self.ruta_archivo_final):
            raise FileNotFoundError(f"No existe el archivo final: '{self.ruta_archivo_final}'")

        ABRIR EL ARCCHIVO EN ruta_archivo_final Y CARGAR UN DATAFRAME DESDE PICKLE

        print(f"[L_Cargar] Dataset final recuperado desde Pickle: '{self.ruta_archivo_final}' ({len(df)} registros).")
        return df


# =============================================================================
# 4. CLASE PIPELINEETL (ORQUESTACIÓN Y GRÁFICO FINAL CONJUNTO)
# =============================================================================
class PipelineETL:
    """
    Clase orquestadora principal:
      1. Utiliza E_Obtener para extraer desde las fuentes y crear los 3 archivos
         intermedios (Apple, Netflix y Messi con filtro MLS).
      2. Utiliza T_Transformar para rellenar datos faltantes mediante aproximaciones
         lineales sobre los valores de E_Obtener y agrupar goles por fecha.
      3. Utiliza L_Cargar para guardar y recuperar el archivo final consolidado.
      4. Genera el gráfico conjunto: BARRAS para goles de Messi y LÍNEAS SUPERPUESTAS
         para los valores de CIERRE de Apple y Netflix.
    """

    def __init__(self):
        self.fecha_inicio: str = "2023-01-01"
        self.fecha_fin: str = "2026-12-31"
        self.ruta_grafico: str = "grafico_messi_apple_netflix.png"

        # Instancias de componentes con los nuevos nombres de clase
        self.extractor = E_Obtener()
        self.cargador = L_Cargar()
        self.transformador = T_Transformar(extractor=self.extractor)

    def ejecutar_pipeline(self) -> pd.DataFrame:
        """
        Ejecuta el pipeline completo cumpliendo todas las directrices:
          - E_Obtener extrae y crea los archivos intermedios de Messi (filtrando MLS), Apple y Netflix.
          - T_Transformar rellena datos faltantes con aproximaciones lineales y agrupa por fecha.
          - L_Cargar persiste el resultado final en Pickle.
        """
        print("\n" + "=" * 80)
        print("EJECUTANDO PIPELINE ETL (CLASES: E_Obtener, T_Transformar, L_Cargar)")
        print("=" * 80)

        # ---------------------------------------------------------------------
        # Configuración del pipeline
        # ---------------------------------------------------------------------
        ObtenerDatos = False
        TransformarDatos = True
        CargarDatos = True


        if (ObtenerDatos == True):    
            # ---------------------------------------------------------------------
            # FASE 1: E_Obtener genera archivos intermedios para cada fuente
            # ---------------------------------------------------------------------
            print("\n[Fase 1 - Extracción y Archivos Intermedios (E_Obtener)]")

            # Pasar directamente los elementos del diccionario 'self.extractor.activos'
            for ticker_key, activo_dict in self.extractor.activos.items():
                self.extractor.obtener_datos_polygon(activo_dict)

            # Extraer exclusivamente goles de Messi en MLS y generar intermedio
            self.extractor.extraer_goles_messi_intermedio()
        else:
            print("\n[Fase 1 - Extracción y Archivos Intermedios (E_Obtener)] SALTEADA")

        if (TransformarDatos == True):
            # ---------------------------------------------------------------------
            # FASE 2: T_Transformar carga intermedios, aplica aproximaciones lineales y transforma
            # ---------------------------------------------------------------------
            print("\n[Fase 2 - Carga de Intermedios y Transformación (T_Transformar)]")

            # Cargar los intermedios usando los métodos de E_Obtener
            df_messi_inter = self.extractor.obtener_intermedio(self.extractor.info_messi["ruta_intermedia"])
            df_apple_inter = self.extractor.obtener_intermedio(self.extractor.activos["AAPL"]["ruta_intermedia"])
            df_netflix_inter = self.extractor.obtener_intermedio(self.extractor.activos["NFLX"]["ruta_intermedia"])

            # Transformar aplicando aproximaciones lineales sobre los datos obtenidos
            df_final = self.transformador.transformar_datos(
                df_messi_intermedio=df_messi_inter,
                df_apple_intermedio=df_apple_inter,
                df_netflix_intermedio=df_netflix_inter
            )
        else:
            print("\n[Fase 2 - Carga de Intermedios y Transformación (T_Transformar)] SALTEADA")

        if (CargarDatos == True):
            # ---------------------------------------------------------------------
            # FASE 3: L_Cargar persiste y valida el archivo final
            # ---------------------------------------------------------------------
            print("\n[Fase 3 - Persistencia del Dataset Final (L_Cargar)]")
            self.cargador.guardar_datos_finales(df_final)

            df_recuperado = self.cargador.cargar_datos_finales()

            print("\n" + "=" * 80)
            print("SE RECUPERARON LOS DATOS PROSESADOS")
            print("=" * 80)
        
            return df_recuperado
        else:
            print("\n[Fase 3 - Persistencia del Dataset Final (L_Cargar)] SALTEADA")


        print("\n" + "=" * 80)
        print("PIPELINE ETL FINALIZADO")
        print("=" * 80)
        
        return None

    def generar_grafico_conjunto(self) -> Dict[str, Any]:
        """
        Genera el gráfico conjunto:
          - BARRAS para los goles de Messi (sumados por fecha).
          - LÍNEAS SUPERPUESTAS con los valores de CIERRE de Apple y Netflix.
        """
        print("\n" + "=" * 80)
        print("GENERANDO GRÁFICO CONJUNTO (BARRAS GOLES + LÍNEAS VALORES DE CIERRE)")
        print("=" * 80)

        df = self.cargador.cargar_datos_finales()
        if df.empty:
            raise ValueError("El DataFrame cargado está vacío.")

        fechas = df["Fecha_Partido"].dt.to_pydatetime()
        goles = df["Goles_Messi"].values
        cierre_apple = df["Apple_Precio_Cierre"].values
        cierre_netflix = df["Netflix_Precio_Cierre"].values

        CREAR GRAFICO PARA COMPARAR GOLES DE MESSI CON EL VALOR DE CIERRE DE LAS ACCIONES


# =============================================================================
# BLOQUE DE EJECUCIÓN PRINCIPAL
# =============================================================================
if __name__ == "__main__":
    pipeline = PipelineETL()
    df_resultado = pipeline.ejecutar_pipeline()
    pipeline.generar_grafico_conjunto()
