"""
Este módulo define a las funciones que extraen la metadata (extración full)
y los mensajes metar (extracción full e incremental) de la estación
meteorológica seleccioanda desde la API, a partir de los módulos
correspondientes.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta


def station_data(base_url, endpoint, aeropuerto):
    """
    Permite extraer la metadata de la estación seleccionada.

    :param base_url: Indica la URL de la API.
    :param endpoint: Indica el "endpoint" que se va a extraer de la API.
    :param aeropuerto: Indica el código OACI del aeropuerto.

    :var endpoint_url: Genera la dirección URL de fomra completa.
    :var params: Contiene los parámetros para la API.
    :var data: Contiene la información provista por la API.

    :type [base_url]: [string]
    :type [endpoint]: [string]
    :type [aeropuerto]: [string]
    :type [endpoint_url]: [string]
    :type [params]: [dict]
    :type [data]: [list]

    :return: [Metadatos de la estación seleccionada]
    :rtype: [pandas.core.frame.DataFrame]

    Los parámetros de la API para este endpoint se pueden consultar en:
    https://aviationweather.gov/data/api/#/Data/dataStationInfo
    """

    try:
        endpoint_url = f"{base_url}/{endpoint}"

        params = {"ids": aeropuerto,
                "bbox": "-20,-80,-60,-40", # latN,LonW,LatS,LonE
                "format": "json"}

        response = requests.get(endpoint_url, params)
        # Levanta una excepción si hay un error en la respuesta HTTP.
        response.raise_for_status()  

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()
            df_station = pd.json_normalize(data)
            
            return df_station[['icaoId','lat','lon','elev','country']]
        except:
            print("El formato de respuesta no es el esperado.")
            return None        

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"La petición ha fallado. Código de error : {e}")
        return None


def data_full(base_url, endpoint, aeropuerto):
    """
    Permite extraer de forma completa los datos de
    temperatura [ºC] de la estación seleccionada.

    :param base_url: Indica la URL de la API.
    :param endpoint: Indica el "endpoint" que se va a extraer de la API.
    :param aeropuerto: Indica el código OACI del aeropuerto.

    :var last_date: Toma la fecha y hora actual en UTC.
    :var horas: Cantidad de horas desde las 00UTC incluyendo a la misma.
    :var endpoint_url: Genera la dirección URL de fomra completa.
    :var params: Contiene los parámetros para la API.
    :var data: Contiene la información provista por la API.

    :type [base_url]: [string]
    :type [endpoint]: [string]
    :type [aeropuerto]: [string]
    :type [endpoint_url]: [string]
    :type [params]: [dict]
    :type [data]: [list]
    :type [last_date]: [string]
    :type [horas]: [integer]

    :return: [Datos de la estación seleccionada de 00Z a hora presente]
    :rtype: [pandas.core.frame.DataFrame]

    Los parámetros de la API para este endpoint se pueden consultar en:
    https://aviationweather.gov/data/api/#/Data/dataMetars
    """

    last_date = datetime.utcnow() # timedelta(hours=1)
    last_date = last_date.strftime("%Y-%m-%dT%H:00:00Z")

    # Calcula la diferencia de horas entre la actual y el inicio del dia
    horas = int(last_date[11:13])+1 # "+1" Para incliur el dato de 00Z
    
    try:
        endpoint_url = f"{base_url}/{endpoint}"

        params = {"ids": "SAEZ",
                "format": "json",
                "taf": False,
                "hours": horas,  # Horas hacia a atrás a devolver
                "bbox": "-20,-80,-60,-40", # latN,LonW,LatS,LonE
                "date": last_date  # yyyy-mm-ddThh:mm:ssZ
                }

        response = requests.get(endpoint_url, params)
        # Levanta una excepción si hay un error en la respuesta HTTP.
        response.raise_for_status()

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()
            df_metar_full = pd.json_normalize(data)
            
            return df_metar_full[['reportTime','temp']][1:]
        except:
            print("El formato de respuesta no es el esperado.")
            return None        

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"La petición ha fallado. Código de error : {e}")
        return None


def data_incremental(base_url, endpoint, aeropuerto):
    """
    Permite extraer de forma incremental de forma horaria
    los datos de temperatura [ºC] de la estación seleccionada.

    :param base_url: Indica la URL de la API.
    :param endpoint: Indica el "endpoint" que se va a extraer de la API.
    :param aeropuerto: Indica el código OACI del aeropuerto.

    :var last_date: Toma la fecha y hora actual en UTC.
    :var endpoint_url: Genera la dirección URL de fomra completa.
    :var params: Contiene los parámetros para la API.
    :var data: Contiene la información provista por la API.

    :type [base_url]: [string]
    :type [endpoint]: [string]
    :type [aeropuerto]: [string]
    :type [endpoint_url]: [string]
    :type [params]: [dict]
    :type [data]: [list]
    :type [last_date]: [string]
    :type [horas]: [integer]

    :return: [Datos de la estación seleccionada de 00Z a hora presente]
    :rtype: [pandas.core.frame.DataFrame]

    Los parámetros de la API para este endpoint se pueden consultar en:
    https://aviationweather.gov/data/api/#/Data/dataMetars
    """

    last_date = datetime.utcnow()
    last_date = last_date.strftime("%Y-%m-%dT%H:00:00Z")

    try:
        endpoint_url = f"{base_url}/{endpoint}"

        params = {"ids": "SAEZ",
                "format": "json",
                "taf": False,
                "hours": 1,  # Horas hacia a atrás a devolver
                "bbox": "-20,-80,-60,-40", # latN,LonW,LatS,LonE
                "date": last_date  # yyyy-mm-ddThh:mm:ssZ
                }

        response = requests.get(endpoint_url, params)
        # Levanta una excepción si hay un error en la respuesta HTTP.
        response.raise_for_status()

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()

            df_metar_incremental = pd.json_normalize(data)

            return df_metar_incremental[['reportTime','temp']]
        except:
            print("El formato de respuesta no es el esperado.")
            return None        

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"La petición ha fallado. Código de error : {e}")
        return None
