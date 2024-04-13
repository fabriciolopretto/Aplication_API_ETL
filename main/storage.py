"""
Este módulo define a las funciones que almacenan en el Data Lake
los datos extraídos de la API.
Se genera directorios para:
1 - Para la metadata de la estación.
2 - Para la extracción full de los datos de temperatura [ºC]
    previos a la hora presente.
3 - Para la extracción incremental de los datos de
    temperatura [ºC] de la hora presente.
"""

# Se importan las librerias necesarias
import pandas as pd
import os
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import configparser
from pprint import pprint

def almacena_full(df, layer, endpoint, aeropuerto):
    """
    Permite almacenar en Data Lake la metadata y datos extraídos de
    forma full.

    :param df: Indica el data frame con los datos.
    :param layer: Capa del Data Lake.
    :param endpoint: Indica el destino dentro del Data Lake.
    :param aeropuerto: Indica el código OACI del aeropuerto.

    :var ruta: Toma la ruta donde se encuentra el script.
    :var landing: Describe ñla ruta donde se guarda el archivo parquet.

    :type [df]: [pandas.core.frame.DataFrame]
    :type [endpoint]: [string]
    :type [aeropuerto]: [string]
    :type [ruta]: [string]
    :type [landing]: [string]

    :return: [Metadatos o Datos Full de la estación seleccionada]
    :rtype: [parquet]
    """

    try:
        ruta = os.path.dirname((os.path.abspath(__file__)))
        landing = ruta+'/data_lake/'+ layer +'/aviationweather_api/'+endpoint+'/'+aeropuerto

        df.to_parquet(
            landing+"/data_parquet",
            engine="fastparquet",

            partition_cols=None
            )
    except:
        print("No se pudo guardar el archivo.")


def almacena_part(df, layer, endpoint, aeropuerto, part):
    """
    Permite almacenar en Data Lake los datos de temperatura [ºC]
    extraídos de forma incremental de forma horaria.

    :param df: Indica el data frame con los datos.
    :param layer: Capa del Data Lake.
    :param endpoint: Indica el destino dentro del Data Lake.
    :param aeropuerto: Indica el código OACI del aeropuerto.

    :var ruta: Toma la ruta donde se encuentra el script.
    :var landing: Describe la ruta donde se guarda el archivo parquet.

    :type [df]: [pandas.core.frame.DataFrame]
    :type [endpoint]: [string]
    :type [aeropuerto]: [string]
    :type [ruta]: [string]
    :type [landing]: [string]

    :return: [Metadatos o Datos Full de la estación seleccionada]
    :rtype: [parquet]
    """
    
    try:
        ruta = os.path.dirname((os.path.abspath(__file__)))
        landing = ruta+'/data_lake/' + layer + '/aviationweather_api/'+endpoint+'/'+aeropuerto+'/'+part
        if not os.path.exists(landing):
            os.makedirs(landing)

        df.to_parquet(
            landing+"/data_parquet",
            engine="fastparquet",

            partition_cols=None
            )
    except:
        print("No se pudo guardar el archivo.")
    

def particion(df):
    """
    Devuelve la fecha y hora de la partición.

    :param df: Indica el data frame "landing" con los datos.

    :return: [Cadena de caracteres con la fecha y hora de la partición]
    :rtype: [str]
    """

    try: 
        particion = str(df.reportTime)
        particion = particion[5:15]+'-'+particion[16:18]+'Z'

        return particion
    
    except:
        print("No se pudo generar el nombre de la partición.")
        return None


def werehouse_save(filename, df1, df2, df3, df4):
    """
    Almacena en el Data Werehouse creado en el servicio de Aiven.

    :param filename: Indica el DataFrame con los datos de la estación.
    :param df1: Indica el DataFrame con los datos de la estación.
    :param df2: Indica el DataFrame con los datos de temperatura horaria.
    :param df3: Indica el DataFrame con los datos de temperatura máxima.
    :param df4: Indica el DataFrame con los datos de temperatura mínima.

    Se crean las Tablas en un Esquema "público", y se persisten los datos
    almacenados en los DataFrames en Tablas correspondientes. 
    """
    try:
        # Se define la ruta relativa al usuario
        ruta = os.path.dirname((os.path.abspath(__file__)))
        # leer el archivo de configuración
        config = configparser.ConfigParser()
        config.read(ruta + '/' + filename)

        # Leer la configuración de la sección 'database'
        db_config = config['postgres']

        # Obtener los valores de configuración
        host = db_config['host']
        port = db_config['port']
        database_name = db_config['dbname']
        username = db_config['user']
        password = db_config['pwd']

        # Configurar la conexión a la base de datos PostgreSQL en Aiven
        # El formato de la URL de conexión es 'postgresql://usuario:contraseña@nombre_de_host:puerto/nombre_de_base_de_datos'
        # Asegurarse de reemplazar los valores con las credenciales
        conn_string = 'postgresql://' + username + ':' + password + '@' + host + ':' + port + '/' + database_name

        # Se crea y se establece la conexion con la base de datos
        engine = create_engine(conn_string)
        

        # Creación de Tablas
        # Se decribe la consulta que crea la tabla de metadatos de las estacion
        query = text("""
        CREATE TABLE IF NOT EXISTS public.metadata_estaciones (
            auto_id SERIAL PRIMARY KEY, -- ID autoincremental
            ICAO TEXT,
            Latitud REAL,
            Longitud REAL,
            MSNM INT,
            País TEXT
        );
        """)

        # Se ejecuta la consulta que crea la tabla de metadatos de la estacion
        with engine.connect() as conn, conn.begin():
            conn.execute(query)

        # Se decribe la consulta que crea la tabla de datos horarios de temperatura
        query = text("""
        CREATE TABLE IF NOT EXISTS public.data_temperature (
            auto_temp_id SERIAL PRIMARY KEY, -- ID autoincremental
            Temperatura REAL,
            Año INT,
            Mes INT,
            Día INT,
            Hora INT,
            Minuto INT
        );
        """)

        # Se ejecuta la consulta que crea la tabla de datos horarios de temperatura
        with engine.connect() as conn, conn.begin():
            conn.execute(query)

        # Se decribe la consulta que crea la tabla donde se registran los valores de temperatura máxima
        query = text("""
        CREATE TABLE IF NOT EXISTS public.max_temperature (
            auto_max_id SERIAL PRIMARY KEY, -- ID autoincremental
            Máxima REAL,
            Año INT,
            Mes INT,
            Día INT,
            Hora INT,
            Minuto INT
        );
        """)

        # Se ejecuta la consulta que crea la tabla donde se registran los valores de temperatura maxima
        with engine.connect() as conn, conn.begin():
            conn.execute(query)

        # Se decribe la consulta que crea la tabla donde se registran los valores de temperatura minima
        query = text("""
        CREATE TABLE IF NOT EXISTS public.min_temperature (
            auto_min_id SERIAL PRIMARY KEY, -- ID autoincremental
            Mínima REAL,
            Año INT,
            Mes INT,
            Día INT,
            Hora INT,
            Minuto INT
        );
        """)

        # Se ejecuta la consulta que crea la tabla donde se registran los valores de temperatura máxima
        with engine.connect() as conn, conn.begin():
            conn.execute(query)

        # Insertado sobre las Tablas
        # Metadatos de la estacion
        with engine.begin() as connection:
            for index, row in df1.iterrows():
                # Consultar si el registro ya existe en la tabla
                consulta = f"SELECT COUNT(*) FROM public.metadata_estaciones WHERE ICAO = '{row['ICAO']}'"
                query = text(consulta)
                result = connection.execute(query).scalar()
                
                # Si el registro no existe, insertarlo en la tabla
                if result == 0:
                    insert_consulta = f"INSERT INTO public.metadata_estaciones (ICAO, Latitud, Longitud, MSNM, País) VALUES ('{row['ICAO']}', {row['Latitud']}, {row['Longitud']}, {row['MSNM']}, '{row['País']}')"
                    insert_query = text(insert_consulta)
                    with engine.connect() as conn, conn.begin():
                        result = conn.execute(insert_query)

        # Datos horarios de temperatura
        with engine.begin() as connection:
            for index, row in df2.iterrows():
                # Consultar si el registro ya existe en la tabla
                consulta = f"SELECT COUNT(*) FROM public.data_temperature WHERE Año = {row['Año']} AND Mes = {row['Mes']} AND Día = {row['Día']} AND Hora = {row['Hora']} AND Minuto = {row['Minuto']}"
                query = text(consulta)
                result = connection.execute(query).scalar()
                
                # Si el registro no existe, insertarlo en la tabla
                if result == 0:
                    insert_consulta = f"INSERT INTO public.data_temperature (Temperatura, Año, Mes, Día, Hora, Minuto) VALUES ({row['Temperatura']}, {row['Año']}, {row['Mes']}, {row['Día']}, {row['Hora']}, {row['Minuto']})"
                    insert_query = text(insert_consulta)
                    with engine.connect() as conn, conn.begin():
                        result = conn.execute(insert_query)

        # Datos de temperatura maxima
        with engine.begin() as connection:
            for index, row in df3.iterrows():
                # Consultar si el registro ya existe en la tabla
                consulta = f"SELECT COUNT(*) FROM public.max_temperature WHERE Año = {row['Año']} AND Mes = {row['Mes']} AND Día = {row['Día']} AND Hora = {row['Hora']} AND Minuto = {row['Minuto']}"
                query = text(consulta)
                result = connection.execute(query).scalar()
                
                # Si el registro no existe, insertarlo en la tabla
                if result == 0:
                    insert_consulta = f"INSERT INTO public.max_temperature (Máxima, Año, Mes, Día, Hora, Minuto) VALUES ({row['Máxima']}, {row['Año']}, {row['Mes']}, {row['Día']}, {row['Hora']}, {row['Minuto']})"
                    insert_query = text(insert_consulta)
                    with engine.connect() as conn, conn.begin():
                        result = conn.execute(insert_query)

        # Datos de temperatura minima
        with engine.begin() as connection:
            for index, row in df4.iterrows():
                # Consultar si el registro ya existe en la tabla
                consulta = f"SELECT COUNT(*) FROM public.min_temperature WHERE Año = {row['Año']} AND Mes = {row['Mes']} AND Día = {row['Día']} AND Hora = {row['Hora']} AND Minuto = {row['Minuto']}"
                query = text(consulta)
                result = connection.execute(query).scalar()
                
                # Si el registro no existe, insertarlo en la tabla
                if result == 0:
                    insert_consulta = f"INSERT INTO public.min_temperature (Mínima, Año, Mes, Día, Hora, Minuto) VALUES ({row['Mínima']}, {row['Año']}, {row['Mes']}, {row['Día']}, {row['Hora']}, {row['Minuto']})"
                    insert_query = text(insert_consulta)
                    with engine.connect() as conn, conn.begin():
                        result = conn.execute(insert_query)

    except:
        print('No fue posible persistir el/los DataFrames en la Base de Datos de Aiven.')


def werehouse_read(filename, tabla):
    """
    Lee los datos almacenados en el Data Werehouse
    creado en el servicio de Aiven.

    :param filename: Indica el DataFrame con los datos de la estación.
    :param tabla: Indica la Tabla del Data Werehouse en Aiven que se va a leer.

    :return: [Metadatos o Datos de la estación seleccionada]
    :rtype: [pandas.core.frame.DataFrame]
    """

    try:

        # Se define la ruta relativa al usuario
        ruta = os.path.dirname((os.path.abspath(__file__)))
        # leer el archivo de configuración
        config = configparser.ConfigParser()
        config.read(ruta + '/' + filename)

        # Leer la configuración de la sección 'database'
        db_config = config['postgres']

        # Obtener los valores de configuración
        host = db_config['host']
        port = db_config['port']
        database_name = db_config['dbname']
        username = db_config['user']
        password = db_config['pwd']

        # Configurar la conexión a la base de datos PostgreSQL en Aiven
        # El formato de la URL de conexión es 'postgresql://usuario:contraseña@nombre_de_host:puerto/nombre_de_base_de_datos'
        # Asegurarse de reemplazar los valores con las credenciales
        database_url = 'postgresql://' + username + ':' + password + '@' + host + ':' + port + '/' + database_name

        # Consulta SQL para seleccionar datos
        query = 'SELECT * FROM ' + tabla

        # Crear el motor de SQLAlchemy
        engine = create_engine(database_url)

        # Ejecutar la consulta SQL y cargar los resultados en un DataFrame
        df = pd.read_sql(query, engine)

        # Mostrar el DataFrame a modo de prueba
        print(df)
    
    except:

        print('No fue posible conectar con la Base de Datos.')
