"""
Este script llama a los módulos (y sus funcionalidades) de extracción
"full" e "incremental" de datos de la API, al de almacenamiento de los
mismos en un Data Lake y Data Werehouse, y al de procesamiento,
transformaición y agregación.
"""

# Se importan los modulos necesarios
from pprint import pprint
from extraction import station_data, data_full, data_incremental
from storage import almacena_full, almacena_part, particion, werehouse_save, werehouse_read
from prosecution import duplicados, elimnar_reportes_nulos, imputacion_datos_nulos, separador_campo_time, formato_datos, formato_metadatos, modificar_campos, imputacion_metadatos_nulos, concatenacion, maxima, minima

# Se le da acceso al editor
if __name__ == "__main__":
    # Se ingresan el URL de la API y los endpoints de interes
    base_url = "https://aviationweather.gov"
    endpoint_aero = "api/data/stationinfo"
    endpoint_metar = "api/data/metar"
    aeropuerto = "SAEZ"

    # EXTRACCION
    # Dataframe de extraccion full de la metadata de la estacion
    df_station_metadata_landing = station_data(base_url, endpoint_aero, aeropuerto)
    # Dataframe de extracción full de datos de la estacion
    df_station_data_full_landing = data_full(base_url, endpoint_metar, aeropuerto)
    # Dataframe de extraccion incremental de datos de la estacion
    df_station_data_incremental_landing = data_incremental(base_url, endpoint_metar, aeropuerto)
    
    # ALMACENAMIENTO (Capa Landing/Bronce)
    # Se almacena la metadata de la estacion meteorologica
    almacena_full(df_station_metadata_landing, layer='landing', endpoint='stations', aeropuerto=aeropuerto)
    # Se almacena los mensajes metar de horas previas
    almacena_full(df_station_data_full_landing, layer='landing', endpoint='mesure/full', aeropuerto=aeropuerto)
    # Se almacena los mensajes metar de la ultima hora
    part = particion(df_station_data_incremental_landing)
    almacena_part(df_station_data_incremental_landing, layer='landing', endpoint='mesure/incre', aeropuerto=aeropuerto, part=part)
    
    # PROCESAMIENTO
    # Se usan los DataFrame "Landing" punto de partida para los "trusted"
    df_station_metadata_trusted = df_station_metadata_landing
    df_station_data_full_trusted = df_station_data_full_landing
    df_station_data_incremental_trusted = df_station_data_incremental_landing
    # Procesamiento Metadatos:
    # Tratamiento de faltantes en los metadatos
    df_station_metadata_trusted = imputacion_metadatos_nulos(df_station_metadata_trusted)
    # Reasignacion de tipo de dato y campos
    df_station_metadata_trusted = formato_metadatos(df_station_metadata_trusted)
    df_station_metadata_trusted = modificar_campos(df_station_metadata_trusted, campos_nuevos=['ICAO','Latitud','Longitud','MSNM','País'])
    # Procesamiento Datos:
    # Eliminacion de registros duplicados
    df_station_data_full_trusted = duplicados(df_station_data_full_trusted)
    # Eliminacion de registros con fecha/hora nulos
    df_station_data_full_trusted = elimnar_reportes_nulos(df_station_data_full_trusted, campo='reportTime')
    df_station_data_incremental_trusted = elimnar_reportes_nulos(df_station_data_incremental_trusted, campo='reportTime')
    # Asignacion de valor "NaN" a los registros con valores faltantes de temperatura
    df_station_data_full_trusted = imputacion_datos_nulos(df_station_data_full_trusted, campo='temp')
    df_station_data_incremental_trusted = imputacion_datos_nulos(df_station_data_incremental_trusted, campo='temp')
    # Separacion de datos de fecha y hora
    df_station_data_full_trusted = separador_campo_time(df_station_data_full_trusted, campo='reportTime')
    df_station_data_incremental_trusted = separador_campo_time(df_station_data_incremental_trusted, campo='reportTime')
    # Reasignacion de los tipos de datos
    df_station_data_full_trusted = formato_datos(df_station_data_full_trusted)
    df_station_data_incremental_trusted = formato_datos(df_station_data_incremental_trusted)
    # Reasignacion nombre campos
    df_station_data_full_trusted = modificar_campos(df_station_data_full_trusted, campos_nuevos=['Temperatura','Año','Mes','Día','Hora','Minuto'])
    df_station_data_incremental_trusted = modificar_campos(df_station_data_incremental_trusted, campos_nuevos=['Temperatura','Año','Mes','Día','Hora','Minuto'])

    # ALMACENAMIENTO (Capa Trusted/Plata)
    # Se almacena la metadata de la estacion meteorologica
    almacena_full(df_station_metadata_trusted, layer='trusted', endpoint='stations', aeropuerto=aeropuerto)
    # Se almacena los mensajes metar de horas previas
    almacena_full(df_station_data_full_trusted, layer='trusted', endpoint='mesure/full', aeropuerto=aeropuerto)
    # Se almacena los mensajes metar de la ultima hora
    almacena_part(df_station_data_incremental_trusted, layer='trusted', endpoint='mesure/incre', aeropuerto=aeropuerto, part=part)

    # PROCESAMIENTO
    # Se usan los DataFrame "trusted" punto de partida para los "refined" (se unen los datos de extraccióon full e incremental)
    df_station_metadata_refined = df_station_metadata_trusted
    df_station_data_total_refined = concatenacion(df_station_data_full_trusted, df_station_data_incremental_trusted)
    # Agregaciones
    df_maximas_refined = maxima(df_station_data_total_refined)
    df_minimas_refined = minima(df_station_data_total_refined)

    # ALMACENAMIENTO (Capa Refined/Oro)
    # Se almacena la metadata de la estacion meteorologica
    almacena_full(df_station_metadata_refined, layer='refined', endpoint='stations', aeropuerto=aeropuerto)
    # Se almacena los mensajes metar de horas previas
    almacena_full(df_station_data_total_refined, layer='refined', endpoint='mesure', aeropuerto=aeropuerto)
    # Archivo con los credenciales
    filename='config.ini'
    # Almacenamiento en Data Werehouse alojado en Aiven
    werehouse_save(filename, df_station_metadata_refined, df_station_data_total_refined, df_maximas_refined, df_minimas_refined)
    # Lectura de tablas del Data Werehouse alojado en Aiven
    werehouse_read(filename)

    
    # Visualizacion de prueba de los DataFrames que se guardan en la capa "refined" deñ Data Lake
    pprint(df_station_metadata_refined)
    pprint(df_station_data_total_refined)
    pprint(df_maximas_refined)
    pprint(df_minimas_refined)
    