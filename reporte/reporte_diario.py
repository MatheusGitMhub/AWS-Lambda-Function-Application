from datetime import datetime
import boto3
import pandas as pd
import numpy as np
import pytz
from report_library import *
from misma_carga_v2 import *
from crear_carpeta import crear_carpeta
import warnings
warnings.filterwarnings("ignore")
import pickle

# Set environment variables
os.environ['inputbucket'] = 'cosmos-anglo-bronces-entrada'

def reporte_lambda(event, context):

    print(event)
    # # Verifica si las variables han sido ingresadas como un testeo
    try:
        # Fecha del reporte de abastecimiento de combustible
        fecha_actual = str(event["Fecha actual"])
        fecha_actual = datetime.strptime(str(fecha_actual), "%Y-%m-%d")

    except:

        try:
            fecha_actual = str(event[-1])
            fecha_actual = datetime.strptime(str(fecha_actual), "%Y-%m-%d")

        except:
            # Fecha del reporte de abastecimiento de combustible
            fecha_actual = datetime.now()
            fecha_actual = fecha_actual - timedelta(days=1)
            fecha_actual = datetime.strptime(str(fecha_actual)[0:10], "%Y-%m-%d")

    # fecha_actual = datetime.strptime("2020-03-01", "%Y-%m-%d")

    # Creamos la carpeta
    # crear_carpeta("tmp")

    # Conexion S3
    s3 = boto3.client("s3")

    # Definimos la fecha
    timezone = pytz.timezone("America/Santiago")
    fecha = datetime.now(timezone)
    fecha = datetime.now()
    fecha = fecha - timedelta(days=14)
    fecha = fecha - timedelta(hours=6)
    fecha = fecha - timedelta(minutes=15)
    fecha = "05-06-2020 10:06:47"
    fecha = datetime.strptime(fecha, "%d-%m-%Y %H:%M:%S")
    fecha = fecha - timedelta(days=1)
    fecha = fecha.replace(tzinfo=timezone)

    day = fecha.day
    month = fecha.month
    year = fecha.year
    hour = fecha.hour
    minute = fecha.minute


    # Leer datos de combustible de los ultimos 2 dias
    combustible = response_s3(s3, fecha, 2, "fueling")

    combustible.columns = ['Transaction_ID', 'Date', 'Shiftday', 'Cantidad',
                           'Tag', 'Equipo', 'Flota', 'Station Name']

        # Calculamos los tiempos entre carga de combustible
    combustible = diff_cargas_combustible(combustible)

    # Aplicamos el codigo de concatenacion de cargas
    combustible = misma_carga_v2(combustible)

    # Volvemos a calcular la diferencia de hora entre las cargas
    combustible = tiempo_entre_cargas(combustible)
    combustible = combustible.drop_duplicates().reset_index(drop=True)

    combustible['Date'] = combustible['Date'].\
        apply(lambda x: x.replace(tzinfo=None))


    # Consumo de solo los camiones caex
    consumo_caex = combustible[combustible['Equipo'].str.contains('CDH')]
    consumo_caex = consumo_caex.sort_values(by=["Equipo", "Date"])
    consumo_caex = consumo_caex.reset_index(drop=True)

    # Conteo de eventos sobre un cierto tiempo determinado
    delta_tiempo = 0.5
    # tiempo que se considera que un camión salió de la mina
    delta_t_salida_mina = 1.5
    # capacidad de los aljibes (según la reu con anglo capacidad máx)
    capacidad_aljibes = 15000
    # flujo con el que descargan los aljibes
    flujo_descarga = 360 # lt/ min


    conteo_eventos_aljibe, frecuencia_cargas =\
        frecuencia_cargas(combustible, delta_tiempo, delta_t_salida_mina)


    # ======================================================================
    # Unión de las predicciones del camión con la del aljibe
    # ======================================================================

    # Porcentaje de combustible antes de irse:
    predicciones = pd.read_csv('consumo_caex_prueba.csv', index_col=0)

    # se debe ingresar el date de predicciones como un datetime
    """
    Aquí falta que lleguen las predicciones de la Pame y pasar las fechas a
    date time
    """
    predicciones['Date'] =\
        predicciones['Date'].apply(lambda x:
                                   datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    # dataframe con todos los procesos de combustible que se realizaron por un
    # aljibe unidas a las predicciones del modelo para camiones
    procesos_combustibles =\
        unir_predicciones_cargas(combustible,
                                 predicciones, delta_t_salida_mina)

    # Dataframe que contiene la información de los ciclos de los aljibes
    ciclos_aljibes =\
        obtener_ciclos_aljibes(procesos_combustibles, delta_t_salida_mina)

    # por cada proceso que realizó un aljibe ir a buscar la info detallada
    # y obtener el reporte
    # ======================================================================
    # Reporte del abastecimiento de los aljibes
    # ======================================================================

    reporte_abastecimiento_aljibes = reporte_aljibes_ciclo(ciclos_aljibes,
                                                     procesos_combustibles,
                                                     fecha,
                                                     flujo_descarga,
                                                     capacidad_aljibes,
                                                     delta_tiempo,
                                                     delta_t_salida_mina)

    reporte_abastecimiento_aljibes.sort_values(by=['Station Name', 'Date'],
                                               inplace=True)

    # Agregar la columna de turno al reporte de aljibes
    reporte_abastecimiento_aljibes['Turno'] =\
        reporte_abastecimiento_aljibes['Date'].apply(aplicacion_turno)

    # filtrar datos para los últimos dos turnos únicamente
    reporte_abastecimiento_aljibes =\
        seleccion_temporal_reporte(reporte_abastecimiento_aljibes, fecha)

    # Separación entre los reportes día y noche del turno
    reporte_noche = reporte_abastecimiento_aljibes[
        reporte_abastecimiento_aljibes['Turno'] == 'N'].reset_index()
    reporte_dia = reporte_abastecimiento_aljibes[
        reporte_abastecimiento_aljibes['Turno'] == 'D'].reset_index()

    # Se lleva el reporte de noche (que incluye horas del día anterior) y
    # el reporte día a S3
    # enviar reporte del día
    root_store_dia =\
        f"indicadores/reporte-abastecimiento/{year}/{month}/\
            reporte-abastecimiento-dia-{year}-{month}-{day}.pkl"
    reporte_dia.to_pickle("/tmp/reporte_abastecimiento_dia.pkl")
    s3.upload_file(Filename="/tmp/reporte_abastecimiento_dia.pkl",
               Bucket="cosmos-anglo-bronces-salida",
               Key=f"{root_store_dia}")

    # enviar reporte de la noche
    root_store_noche =\
        f"indicadores/reporte-abastecimiento/{year}/{month}/\
            reporte-abastecimiento-noche-{year}-{month}-{day}.pkl"
    reporte_noche.to_pickle("/tmp/reporte_abastecimiento_noche.pkl")
    s3.upload_file(Filename="/tmp/reporte_abastecimiento_noche.pkl",
               Bucket="cosmos-anglo-bronces-salida",
               Key=f"{root_store_noche}")
