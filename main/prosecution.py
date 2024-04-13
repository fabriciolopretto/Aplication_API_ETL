"""
Este módulo define a las funciones que realizan en el procesamiento
los datos extraídos de la API. Las tareas realizadas son:
1 - Elimina los registros duplicados.
2 - Elimina registros de datos que no tienen fecha/hora resgitrada.
3 - Completa con "NaN" los datos faltantes del campo indicado
4 - Se para el campo de tiempo en año/mes/día/hora/minutos
5 - Modifica el tipo de datos para optimizar en memoria.
6 - 
"""

# Se importan las librerias necesarias
import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np


# Eliminación de registros duplicados
def duplicados(df):
    """
    Elimina los registros duplicados en los datos almacenados
    en los archivos parquet alojados en la capa "Landing" del Data Lake.

    :param df: Indica el data frame con los datos.

    :type [df]: [pandas.core.frame.DataFrame]

    :return: [Metadatos o Datos de la estación seleccionada]
    :rtype: [pandas.core.frame.DataFrame]
    """

    try:
        df_sin_duplicados = df.drop_duplicates()
        return df_sin_duplicados
    
    except:
        print("No se pudo realizar eliminar los registros duplicados.")
        return None        


def elimnar_reportes_nulos(df, campo):
    """
    Elimina los registros sin identificador de fecha y hora en los datos
    almacenados en los archivos parquet, alojados en la capa "Landing"
    del Data Lake.

    :param df: Indica el data frame con los datos.
    :param campo: Indica el campo en el que se detectan faltantes.

    :type [df]: [pandas.core.frame.DataFrame]

    :return: [Meta. o Datos de la estación seleccionada]
    :rtype: [pandas.core.frame.DataFrame]
    """

    try:
        df_sin_fecha_y_hora = df.dropna(subset=[campo])
        return df_sin_fecha_y_hora
    
    except:
        print("No se pudo eliminar registros sin fecha y hora.")
        return None        


def imputacion_metadatos_nulos(df):
    """
    Asigna valores por defecto a los registros almacenados
    en el archivo parquet (alojados en la capa "Landing"
    del Data Lake).

    :param df: Indica el data frame con los datos.

    :type [df]: [pandas.core.frame.DataFrame]

    :return: [Datos de la estación seleccionada]
    :rtype: [pandas.core.frame.DataFrame]
    """

    try:
        imputation_mapping = {
        'icaoId': "Sin Dato",
        'lat': np.nan,
        'lon': np.nan,
        'elev': np.nan,
        'country': "Sin Dato"
        }

        df_impugnado = df.fillna(imputation_mapping)
        return df_impugnado

    except:
        print("No se pudo asignar valores por defecto a los faltantes.")
        return None        


def imputacion_datos_nulos(df, campo):
    """
    Asigna a los registros almacenados en los archivos parquet (alojados
    en la capa "Landing" del Data Lake) con valores de temperatura nulos
    el valor por defecto "NaN" .

    :param df: Indica el data frame con los datos.
    :param campo: Indica el campo (Temp.) en el que se detectan faltantes.

    :type [df]: [pandas.core.frame.DataFrame]

    :return: [Datos de la estación seleccionada]
    :rtype: [pandas.core.frame.DataFrame]
    """

    try:
        imputation_mapping = {
        campo: np.nan
        }

        df_impugnado = df.fillna(imputation_mapping)
        return df_impugnado

    except:
        print("No se pudo asignar valores por defecto a los faltantes.")
        return None        


def separador_campo_time(df, campo):
    """
    A los valores de fecha/hora almacenados en los archivos parquet
    (alojados en la capa "Landing" del Data Lake) los separa en campos
    de: año, mes, día, hora y minutos. Además, elimina el campo original.

    :param df: Indica el data frame con los datos.
    :param campo: Indica el campo (Temp.) en el que se detectan faltantes.

    :type [df]: [pandas.core.frame.DataFrame]

    :return: [Datos de la estación seleccionada]
    :rtype: [pandas.core.frame.DataFrame]    
    """

    try:    
        # Se paramos los elementos por los caracteres ":"," " y "-" 
        df[campo] = df[campo].str.split(r'[: \-]')
        # Asignamos los valores separados a nuevos campos
        df["anno"] = df[campo].str[0]
        df["mes"] = df[campo].str[1]
        df["dia"] = df[campo].str[2]
        df["hora"] = df[campo].str[3]
        df["minuto"] = df[campo].str[4]

        # Descartamos el campo original
        df = df.drop(campo, axis=1)
        return df

    except:
        print("No se pudo separar el reporte de fecha y hora.")
        return None        


def formato_datos(df):
    """
    Modifica el tipo de dato de los Datos almacenados
    en los archivos parquet alojados en el Data Lake.
    Asigna el tipo de dato que mejor se ajusta, para
    minimizar el espacio en memoria.

    :param df: Indica el data frame con los datos.

    :type [df]: [pandas.core.frame.DataFrame]

    :return: [Datos de la estación seleccionada]
    :rtype: [pandas.core.frame.DataFrame]
    """

    try:
        conversion_mapping = {
            "temp": "float",
            "anno": "int16",
            "mes": "int8",
            "dia": "int8",
            "hora": "int8",
            "minuto": "int8",
        }

        df = df.astype(conversion_mapping)
        return df

    except:
        print("No se pudo darle formato a los datos.")
        return None        


def modificar_campos(df, campos_nuevos):
    """
    Modifica ell nombre de las columnas del DataFrame.

    :param df: Indica el data frame con los datos.
    :param campos_nuevos: Lista los nombres nuevos de los campos.

    :return: [Metadatos de la estación seleccionada]
    :rtype: [pandas.core.frame.DataFrame]    
    """
    try:
        df.columns = campos_nuevos
        return df

    except:
        print("No se pudo modificar el nombre de los campos.")
        return None        


def formato_metadatos(df):
    """
    Modifica el tipo de dato de los Metadatos almacenados
    en el archivo parquet alojado en el Data Lake.
    Asigna el tipo de dato que mejor se ajusta, para
    minimizar el espacio en memoria.

    :param df: Indica el data frame con los datos.

    :type [df]: [pandas.core.frame.DataFrame]

    :return: [Metadatos de la estación seleccionada]
    :rtype: [pandas.core.frame.DataFrame]
    """

    try:
        conversion_mapping = {
            "icaoId": "string",
            "lat": "float",
            "lon": "float",
            "elev": "int16",
            "country": "string"
        }

        df = df.astype(conversion_mapping)
        return df

    except:
        print("No se pudo darle formato a los metadatos.")
        return None        


def concatenacion(df1, df2):
    """
    Realiza la concatenación de los DataFrames que tienen los
    registros obtenidos con la extracción full y el obtenido con
    la extracción incremental.

    :param df1: Indica el DataFrame con los datos full.
    :param df1: Indica el DataFrame con los datos incremental.

    :type [df1]: [pandas.core.frame.DataFrame]
    :type [df2]: [pandas.core.frame.DataFrame]

    :return: [Datos de la estación seleccionada concatenados]
    :rtype: [pandas.core.frame.DataFrame]
    """

    try:
        df_concatenado = pd.concat([df1, df2], ignore_index=True)
        return df_concatenado
    
    except:
        print("No se pudo realizar la concatenación.")
        return None        


def maxima(df):
    """
    Calcula la temperatura máxima, guardando en un nuevo
    DataFrame los valores con la fecha y hora en la que sucedió.

    :param df: Indica el DataFrame con los datos full.

    :type [df]: [pandas.core.frame.DataFrame]

    :return: [DataDrame con los valores de temperatura máxima diaria]
    :rtype: [pandas.core.frame.DataFrame]
    """

    try:

        # Se define el DataFrame que resigtra la temperatura máxima
        df_maximas = pd.DataFrame(columns=['Máxima', 'Año', 'Mes', 'Día', 'Hora', 'Minuto'])

        # Se buscan la máxima en el DataFrame de temperaturas horarias
        t_max = df['Temperatura'].max()
        
        # Se Ubica el número de registro para acceder a la fecha y hora
        indice_max_valor = df['Temperatura'].idxmax()
        
        # Se ubican los valores de fecha y hora para ese numero de registro de la maxima
        anno_max = df.loc[indice_max_valor, 'Año']
        mes_max = df.loc[indice_max_valor, 'Mes']
        dia_max = df.loc[indice_max_valor, 'Día']
        hora_max = df.loc[indice_max_valor, 'Hora']
        minuto_max = df.loc[indice_max_valor, 'Minuto']
        
        # Se completa el nuevo DataFrame
        df_maximas.loc[dia_max, 'Máxima'] = t_max
        df_maximas.loc[dia_max, 'Año'] = anno_max
        df_maximas.loc[dia_max, 'Mes'] = mes_max
        df_maximas.loc[dia_max, 'Día'] = dia_max
        df_maximas.loc[dia_max, 'Hora'] = hora_max
        df_maximas.loc[dia_max, 'Minuto'] = minuto_max

        # Se formatean los campos
        conversion_mapping = {
            "Máxima": "float",
            "Año": "int16",
            "Mes": "int8",
            "Día": "int8",
            "Hora": "int8",
            "Minuto": "int8",
        }

        df_maximas = df_maximas.astype(conversion_mapping)

        return df_maximas

    except:

        print("No se pudo calcular y/o almacenar la máxima.")
        return None        


def minima(df):
    """
    Calcula la temperatura mínima, guardando en un nuevo
    DataFrame los valores con la fecha y hora en la que sucedió.

    :param df: Indica el DataFrame con los datos full.

    :type [df]: [pandas.core.frame.DataFrame]

    :return: [DataDrame con los valores de temperatura máxima diaria]
    :rtype: [pandas.core.frame.DataFrame]
    """

    try:

        # Se define el DataFrame que resigtra la temperatura máxima
        df_minimas = pd.DataFrame(columns=['Mínima', 'Año', 'Mes', 'Día', 'Hora', 'Minuto'])

        # Se buscan la máxima en el DataFrame de temperaturas horarias
        t_min = df['Temperatura'].min()
        
        # Se Ubica el número de registro para acceder a la fecha y hora
        indice_min_valor = df['Temperatura'].idxmin()
        
        # Se ubican los valores de fecha y hora para ese numero de registro de la maxima
        anno_min = df.loc[indice_min_valor, 'Año']
        mes_min = df.loc[indice_min_valor, 'Mes']
        dia_min = df.loc[indice_min_valor, 'Día']
        hora_min = df.loc[indice_min_valor, 'Hora']
        minuto_min = df.loc[indice_min_valor, 'Minuto']
        
        # Se completa el nuevo DataFrame
        df_minimas.loc[dia_min, 'Mínima'] = t_min
        df_minimas.loc[dia_min, 'Año'] = anno_min
        df_minimas.loc[dia_min, 'Mes'] = mes_min
        df_minimas.loc[dia_min, 'Día'] = dia_min
        df_minimas.loc[dia_min, 'Hora'] = hora_min
        df_minimas.loc[dia_min, 'Minuto'] = minuto_min

        # Se formatean los campos
        conversion_mapping = {
            "Mínima": "float",
            "Año": "int16",
            "Mes": "int8",
            "Día": "int8",
            "Hora": "int8",
            "Minuto": "int8",
        }

        df_minimas = df_minimas.astype(conversion_mapping)

        return df_minimas

    except:

        print("No se pudo calcular y/o almacenar la mínima.")
        return None        
