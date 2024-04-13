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
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import configparser

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

        # Definir las tablas con SQLAlchemy
        Base_est = declarative_base()
        Base_dat = declarative_base()
        Base_max = declarative_base()
        Base_min = declarative_base()

        class Estacion(Base_est):
            __tablename__ = 'estaciones'
            id_estacion = Column(Integer, primary_key=True, unique=True)
            icao = Column(String)
            latitud = Column(Float)
            longitud = Column(Float)
            altura = Column(Integer)
            pais = Column(String)
        
        class Datos(Base_dat):
            __tablename__ = 'datos'
            id_dato = Column(Integer, primary_key=True, unique=True)
            temperature = Column(Integer)
            anno = Column(Integer)
            mes = Column(Integer)
            dia = Column(Integer)
            hora = Column(Integer)
            minuto = Column(Integer)
        
        class Maximas(Base_max):
            __tablename__ = 'maximas'
            id_max = Column(Integer, primary_key=True, unique=True)
            temp_max = Column(Integer)
            anno = Column(Integer)
            mes = Column(Integer)
            dia = Column(Integer)
            hora = Column(Integer)
            minuto = Column(Integer)
        
        class Minimas(Base_min):
            __tablename__ = 'minimas'
            id_min = Column(Integer, primary_key=True, unique=True)
            temp_min = Column(Integer)
            anno = Column(Integer)
            mes = Column(Integer)
            dia = Column(Integer)
            hora = Column(Integer)
            minuto = Column(Integer)
        
        # Crear una instancia de motor SQLAlchemy
        engine = create_engine(database_url)
        Base_est.metadata.create_all(engine)
        Base_dat.metadata.create_all(engine)
        Base_max.metadata.create_all(engine)
        Base_min.metadata.create_all(engine)

        # Insertar los datos del DataFrame en la tabla
        Session = sessionmaker(bind=engine)
        session = Session()

        for index, row in df1.iterrows():
            estaciones = Estacion(icao=row['ICAO'], latitud=row['Latitud'], longitud=row['Longitud'], altura=row['M.S.N.M.'], pais=row['País'])
            session.add(estaciones)
        
        for index, row in df2.iterrows():
            datos = Datos(temperature=row['Temperatura'], anno=row['Año'], mes=row['Mes'], dia=row['Día'], hora=row['Hora'], minuto=row['Minuto'])
            session.add(datos)
        
        for index, row in df3.iterrows():
            maximas = Maximas(temp_max=row['Máxima'], anno=row['Año'], mes=row['Mes'], dia=row['Día'], hora=row['Hora'], minuto=row['Minuto'])
            session.add(maximas)
        
        for index, row in df4.iterrows():
            minimas = Minimas(temp_min=row['Mínima'], anno=row['Año'], mes=row['Mes'], dia=row['Día'], hora=row['Hora'], minuto=row['Minuto'])
            session.add(minimas)
        
        session.commit()

        # Cerrar la sesión
        session.close()        

    except:

        print('No fue posible conectar con la Base de Datos')


def werehouse_read(filename):
    """
    Lee los datos almacenados en el Data Werehouse
    creado en el servicio de Aiven.

    :param filename: Indica el DataFrame con los datos de la estación.

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

        # Tabla del Data Werehouse
        tabla = 'minimas'

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
