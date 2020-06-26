import pickle
from datetime import timedelta
from datetime import datetime
import pandas as pd
import numpy as np
import pytz
import requests
import json
import io
import boto3
import os
import decimal

def eliminar_ceros(date):
    return date[:10]


def ultima_carga(diferencia):
    return 1 if diferencia > 1.5 else 0


def ultima_carga2(diferencia):
    return 1 if diferencia > 6 else 0


def timezone_fechas(zona, fecha):
    """
    La funcion entrega el formato de zona horaria a las fechas de los
    dataframe

    Parameters
    ----------
    zona: zona horaria a usar
    fecha: fecha a modificar

    Returns
    -------
    fecha_zh: fecha con el fomarto de zona horaria

    """
    # Definimos la zona horaria
    timezone = pytz.timezone(zona)
    fecha_zs = timezone.localize(fecha)

    return fecha_zs


def timezone_np(t, zona):
    """
    Recibe una fecha en formato numpy.datetime y la transforma a
    el formato datetime con la zona correspondiente

    Parameters
    ----------
    t: fecha a modificar
    zona: zona horaria a usar

    Returns
    -------
    fecha: fecha con el fomarto de zona horaria

    """
    # Numero de segundos en un nanosegundo
    ns = 1e-09
    fecha = datetime.utcfromtimestamp(t.astype(datetime)*ns)
    fecha = timezone_fechas(zona, fecha)
    return fecha


def diff_cargas_combustible(combustible):
    """
    Crear el formato necesario para usar las cargas de combustible

    """
    # Preparamos los datos para unir las cargas
    combustible["diff"] = np.nan
    combustible["carga anterior"] = np.nan
    combustible.reset_index(drop=True, inplace=True)
    combustible["diff"] = combustible.sort_values(['Equipo', 'Date']).\
        groupby('Equipo')['Date'].diff()
    combustible['diff seg'] = combustible['diff'].dt.total_seconds()
    combustible["diff hrs"] = combustible['diff seg']/3600

    return combustible


def response_s3(s3, fecha_actual, dias, ruta_s3):
    """
    La funcion busca los archivos .pickle que cumplen un rango de
    fechas

    Parameters
    ----------
    s3: Conexion a servicio s3
    fecha_actual : Ultima fecha desde la que se analiza
    dias : Dias hacia atras que se quiere buscar
    ruta_s3 : carpeta s3 a la que se quiere ir

    Returns
    -------
    data_final : dataframe con los datos

    """

    # Data final
    data_final = pd.DataFrame()

    # Lista de fechas en el rango
    fechas = []

    bucket = os.environ["inputbucket"]

    # Calculamos los dias anteriores
    for i in range(dias):
        print(i)
        d = (fecha_actual-timedelta(i)).day
        m = (fecha_actual-timedelta(i)).month
        y = (fecha_actual-timedelta(i)).year
        prefijo = str(y) + "/" + str(m) + "/" + str(d) + "/"
        print(prefijo)
        fechas.append(prefijo)

    # Para cada dia
    for fecha in fechas:

        try:
            buffer = io.BytesIO()
            client = boto3.resource("s3")

            object = client.\
                Object(bucket,
                       f"{ruta_s3}-preprocessed/{fecha}{ruta_s3}.parquet")
            object.download_fileobj(buffer)

            data = pd.read_parquet(buffer)
            data_final = pd.concat([data_final, data])

        except Exception as e:
            print(e)

            pass

    return data_final


def tiempo_entre_cargas(combustible):

    combustible["diff"] = combustible.sort_values(['Equipo', 'Date']).\
                        groupby('Equipo')['Date'].diff()
    combustible['diff seg'] = combustible['diff'].dt.total_seconds()
    combustible["diff hrs"] = combustible['diff seg']/3600

    # Dejamos las columnas necesarias
    combustible = combustible[['Station Name', 'Flota', 'Equipo',
                               'Cantidad', 'Date', 'diff hrs']]

    return combustible


def timezone_np(t, zona):
    """
    Recibe una fecha en formato numpy.datetime y la transforma a
    el formato datetime con la zona correspondiente
    Parameters
    ----------
    t: fecha a modificar
    zona: zona horaria a usar
    Returns
    -------
    fecha: fecha con el fomarto de zona horaria
    """
    # Numero de segundos en un nanosegundo
    ns = 1e-09
    fecha = datetime.utcfromtimestamp(t.astype(datetime)*ns)
    fecha = timezone_fechas(zona, fecha)
    return fecha

def reportes_a_dispatch(consumo_caex, fecha,
                        estanque_camiones=4542,
                        delta_t=10):
  
    # Consumo de los camiones antes de ir a cargar
    consumo_caex['antes_carga'] =\
        (estanque_camiones - consumo_caex['Cantidad']) / estanque_camiones *100
    # Camiones que fueron a cargar con más del 40 % de combustible
    errror_dispatch =\
        consumo_caex[(consumo_caex['antes_carga'] > 40)]

    day = fecha.day
    month = fecha.month
    year = fecha.year
    
    errror_dispatch['Year'] = errror_dispatch['Date'].dt.year
    errror_dispatch['Month'] = errror_dispatch['Date'].dt.month
    errror_dispatch['Day'] = errror_dispatch['Date'].dt.day

    # errror_dispatch['Date'] = errror_dispatch['Date'].dt.tz_localize(None)

    # Este es el resumen diario que se va a los despachadores
    errror_dispatch_diario =\
        errror_dispatch[(errror_dispatch['Year'] ==year) &
                        (errror_dispatch['Month'] == month) &
                        (errror_dispatch['Day'] == day)]
    
  
    fecha = fecha.replace(tzinfo=None)    
    errror_dispatch['Date'] =\
        pd.to_datetime(errror_dispatch['Date']).\
            apply(lambda x: x.replace(tzinfo=None)) 
      
    errror_dispatch['diff_min'] =\
        abs((errror_dispatch['Date'] - fecha).dt.total_seconds() / 60)

    errror_dispatch_alerta =\
        errror_dispatch[errror_dispatch['diff_min'] <= delta_t]
        
    errror_dispatch_alerta.drop(columns=['diff_min'], inplace=True)
    errror_dispatch.drop(columns=['diff_min'], inplace=True)
    
    errror_dispatch = errror_dispatch.reset_index(drop=True)
    errror_dispatch_alerta = errror_dispatch_alerta.reset_index(drop=True)
    
    return errror_dispatch, errror_dispatch_alerta



def round_float_to_decimal(float_value):
    """
    Convert a floating point value to a decimal that DynamoDB can store,
    and allow rounding.
    """

    # Perform the conversion using a copy of the decimal context that boto3
    # uses. Doing so causes this routine to preserve as much precision as
    # boto3 will allow.
    with decimal.localcontext(boto3.dynamodb.types.DYNAMODB_CONTEXT) as \
        decimalcontext:

        # Allow rounding.
        decimalcontext.traps[decimal.Inexact] = 0
        decimalcontext.traps[decimal.Rounded] = 0
        decimal_value = decimalcontext.create_decimal_from_float(float_value)
        return decimal_value
    
    
def enviar_alertas_dispatch(errror_dispatch_alerta, dynamodb, dynamoTable):
    
    for i in range(errror_dispatch_alerta.shape[0]):
        
        try:
            station_name = errror_dispatch_alerta['Station Name'].iloc[i]
            flota = errror_dispatch_alerta['Flota'].iloc[i]
            equipo = errror_dispatch_alerta['Equipo'].iloc[i]
            cantidad = errror_dispatch_alerta['Cantidad'].iloc[i]
            fecha = errror_dispatch_alerta['Date'].iloc[i]
            porcentaje_carga = errror_dispatch_alerta['antes_carga'].iloc[i]
            
            station_name = str(station_name)
            flota = str(flota)
            equipo = str(equipo)
            cantidad = str(round(cantidad))
            porcentaje_carga = str(round(porcentaje_carga))
                
            items = {'Equipo': {'S': equipo},
                  'Fecha': {'S': fecha},
                  'Cantidad': {'N': cantidad},
                  'Flota': {'S': flota},
                  'Porcentaje': {'N': porcentaje_carga},
                  'Station Name': {'S': station_name}}
            
            dynamodb.put_item(TableName='dispatch_table',
                          ConditionExpression='attribute_not_exists(Fecha)',
                          Item=items)
            
        except dynamodb.exceptions.ConditionalCheckFailedException:
            print("Este registro ya existe en la BD")
            


def frecuencia_cargas(combustible, delta_tiempo, delta_t_salida_mina):
    """
    1- Cálcula la frecuencia con la que cargan los aljibes en la mina
    2- Cálcula un conteo de eventos por u sobre un nivel de tiempo
        establecido que se considera como perdida de tiempo. 

    Parameters
    ----------
    combustible : Dataframe
        Transaciones de combustible
    delta_tiempo : int
        tiempo por el sobre los cuales se van a contar como eventos malos
    delta_t_salida_mina : int
        tiempo en el que un camión pasa sin cargar y se considera que salió 
        de la mina

    Returns
    -------
    conteo de eventos por sobre delta_tiempo por aljibe
    frecuencia con la que cargan los aljibes

    """
    cargas_de_aljibes =\
        combustible[combustible['Station Name'].str.contains("jibe")==True]
    
    cargas_de_aljibes = cargas_de_aljibes[['Station Name', 'Flota', 'Equipo',
                                     'Cantidad', 'Date']]
    
    cargas_de_aljibes = cargas_de_aljibes.sort_values(by=['Station Name',
                                                          'Date'])
    cargas_de_aljibes['tiempo_entre_cargas'] =\
        cargas_de_aljibes.groupby('Station Name')['Date'].diff()
    
    cargas_de_aljibes['tiempo_entre_cargas'] =\
        cargas_de_aljibes['tiempo_entre_cargas'].dt.total_seconds() / (60*60)
    
    conteo_eventos_aljibe =\
        cargas_de_aljibes[(cargas_de_aljibes['tiempo_entre_cargas'] > 
                           delta_tiempo) & 
                       (cargas_de_aljibes['tiempo_entre_cargas'] <
                        delta_t_salida_mina)]
        
    
    conteo_eventos_aljibe =\
        conteo_eventos_aljibe[['Station Name',
                               'Equipo']].groupby('Station Name').count()
        
    conteo_eventos_aljibe = conteo_eventos_aljibe.reset_index(drop=False)
    conteo_eventos_aljibe.columns =\
        ["Aljibe", f"Eventos de carga sobre {int(delta_tiempo*60)} min"]
    
    # Frecuencia de cargas
    frecuencia_cargas =\
        cargas_de_aljibes[cargas_de_aljibes['tiempo_entre_cargas'] <=
                          delta_t_salida_mina]
    frecuencia_cargas =\
        frecuencia_cargas.groupby('Station Name').mean().\
            reset_index(drop=False)
    frecuencia_cargas.columns = ["Station Name",
                                 "Cantidad promedio de cargas",
                               "Frecuencia promedio de cargas"]

    return conteo_eventos_aljibe, frecuencia_cargas


def obtener_ciclos_aljibes(procesos_combustibles, delta_t_salida_mina):
    """
    Esta es la función que determina los ciclos de los aljibes en base 
    a la condición temporal que establece la salida de la mina de un aljibe
    
    Parameters
    ----------
    combustible : Dataframe
        dataframe con las columnas de combustible
    predicciones : Dataframe
        dataframe con las predicciones de los camiones        
    delta_t_salida_mina : Int
        tiempo que se considera como una salida de la mina

    Returns
    -------
    ciclos_aljibes: dataframe que contiene los ciclos de cargas de los 
    aljibes

    """
    cargas_combustible = procesos_combustibles.copy()
    
    
    ciclos_aljibes = pd.DataFrame()
    # para cada aljibe que opero hoy sacamos los tiempos de ciclo que tuvieron
    for aljibe in cargas_combustible['Station Name'].unique():

        # Dataframes de todas las cargas del día por aljibe
        aljibe_i =\
            cargas_combustible[cargas_combustible['Station Name'] == aljibe]
        # reset index por seguridad
        aljibe_i = aljibe_i.reset_index(drop=True)
        # Setiar el inicio del turno que es el primer dato
        inicio = pd.DataFrame(aljibe_i.iloc[0]).transpose()
        # Imponer condición sobre las salidas de la mina
        salidas_i =\
            aljibe_i[aljibe_i['tiempo_entre_cargas'] > delta_t_salida_mina]
        salidas_i = pd.concat([inicio, salidas_i])        
        # calculo de los tiempo de ciclo de los aljibes
        salidas_i['tiempo_ciclo_aljibes'] =\
            salidas_i['Date'].diff().dt.total_seconds()
        salidas_i['tiempo_ciclo_aljibes'] =\
            salidas_i['tiempo_ciclo_aljibes'] /3600
        # No considero el inicio
        salidas_i = pd.DataFrame(salidas_i.iloc[1:])
        salidas_i.reset_index(drop=True, inplace=True)
        ciclos_aljibes = pd.concat([ciclos_aljibes, salidas_i])
        
    # ciclos de aljibe tiene toda la info de los los ciclos que se realizaron
    ciclos_aljibes = ciclos_aljibes.reset_index(drop=True)
    
    return ciclos_aljibes

def unir_predicciones_cargas(combustible, predicciones, delta_t_salida_mina):
    """
    Unir las predicciones del modelo con los ciclos de combustible

    Parameters
    ----------
    combustible : Dataframe
        dataframe con las columnas de combustible
    predicciones : Dataframe
        dataframe con las predicciones de los camiones        
    delta_t_salida_mina : Int
        tiempo que se considera como una salida de la mina

    Returns
    -------
    ciclos_aljibes: dataframe que contiene los ciclos de cargas de los 
    aljibes

    """
    # La columna diff hrs tiene el tiempo de ciclo de los equipos
    cargas_combustible = combustible.copy()
    # columnas con las que se hará el merge entre predict y combustible
    columnas = ['Equipo', 'Date']
    # Merge con el predict
    cargas_combustible =\
        predicciones[['Equipo', 'Date', 'Prediccion']].merge(cargas_combustible,
                                                             how='outer',
                                                             on=columnas)

    # sacamos las cargas que realizaron cada uno de los aljibes
    cargas_combustible =\
        cargas_combustible[cargas_combustible['Station Name'].\
                           str.contains("jibe")==True]
    # Ordenar por estación y fecha para poder hacer el groupby después
    cargas_combustible = cargas_combustible.sort_values(by=['Station Name',
                                                            'Date'])
    # tiempo entre las cargas
    cargas_combustible['tiempo_entre_cargas'] =\
        cargas_combustible.groupby('Station Name')['Date'].diff()
    # tiempo entre cargas en horas
    cargas_combustible['tiempo_entre_cargas'] =\
        cargas_combustible['tiempo_entre_cargas'].dt.total_seconds() / 3600
        
    cargas_combustible = cargas_combustible.reset_index(drop=True)
    # rellenar con ceros para que se pueda recorrer
    cargas_combustible['tiempo_entre_cargas'].fillna(0, inplace=True)
    

    cargas_combustible["diff prediccion-real"] =\
        cargas_combustible["Prediccion"] - cargas_combustible["Cantidad"]
        
    return cargas_combustible



def reporte_aljibes_ciclo(ciclos_aljibes, procesos_combustibles,
                    fecha, flujo_descarga, capacidad_aljibes,
                    delta_tiempo, delta_t_salida_mina):

    day = fecha.day
    month = fecha.month
    year = fecha.year
    hour = fecha.hour
    minute = fecha.minute        
    
    # Nuevas variables a incorporar en el reporte
    ciclos_aljibes['gasto_combustible_ciclo'] = 0
    ciclos_aljibes['eventos_sin_carga_completa'] = 0
    ciclos_aljibes['cargas_ciclo'] = 0
    ciclos_aljibes['cargas_a_camiones_ciclo'] = 0
    ciclos_aljibes['litros_a_camiones'] = 0
    ciclos_aljibes['porcentaje_combustible_salida'] = 0
    ciclos_aljibes['estado_salida'] = 0
    ciclos_aljibes['eventos_sobre_tiempo'] = 0
    
    # Hacer el cálculo para cada ciclo de combustible
    for indice in range(ciclos_aljibes.shape[0]):
        
        # Aljibe en que hay 
        aljibe = ciclos_aljibes['Station Name'].iloc[indice]
        # tiempo de ciclo
        tiempo_ciclo = ciclos_aljibes['tiempo_ciclo_aljibes'].iloc[indice]
        # fecha final del ciclo
        fecha_f = ciclos_aljibes['Date'].iloc[indice]
        # fecha de inicio del ciclo
        fecha_i = fecha_f - timedelta(hours=tiempo_ciclo)
        # cargas que realizó el aljibe en el periodo de tiempo de ciclo
        cargadas = procesos_combustibles[
            (procesos_combustibles["Station Name"] == aljibe) &
            (procesos_combustibles["Date"] > fecha_i) &
            (procesos_combustibles["Date"] <= fecha_f)]       
        # ordenar por fechas las cargas realizadas en el periodo
        cargadas = cargadas.sort_values(by=["Date"]).reset_index(drop=True)
        ### Variables que se van agregar al reporte        
        # Litros cargados por el aljibe en este ciclo
        suma_cargas = cargadas['Cantidad'].sum()
        # Dataframe con las cargas realizadas a camiones CAEX únicamente
        camiones = cargadas[cargadas['Equipo'].str.contains('CDH')]
        # cantidad de litros de error que puede haber entre el modelo y 
        # la realidad para que sea considerado como una carga incompleta
        delta_litros = 600
        eventos_estanque =\
            camiones[camiones['diff prediccion-real'] > delta_litros].shape[0]       
        # Numero de cargas y litros cargados a camiones CAEX
        cargas_camiones = camiones.shape[0]
        litros_camiones = camiones["Cantidad"].sum()
        # Contar la cantidad de cargas que realiza el aljibe
        numero_cargas = cargadas.shape[0]
        # contar canidad de veces que esta sobre delta_tiempo y menor a 
        eventos_sobre_tiempo = cargadas[
            (cargadas['tiempo_entre_cargas'] > delta_tiempo) &
            (cargadas['tiempo_entre_cargas'] < delta_t_salida_mina)].shape[0]

        # Nueva variable de carga
        # los aljibes pueden retirarse solo cuando han consumido un 96% de su 
        # capacidad
        if suma_cargas >= capacidad_aljibes*0.9666:
            estado_aljibe = "Aljibe agota combustible"
        else:
            estado_aljibe = "Aljibe sale de la mina con combustible"
            
        ciclos_aljibes['gasto_combustible_ciclo'].iloc[indice] = suma_cargas
        ciclos_aljibes['eventos_sin_carga_completa'].iloc[indice] =\
            eventos_estanque
        ciclos_aljibes['cargas_ciclo'].iloc[indice] = numero_cargas
        ciclos_aljibes['cargas_a_camiones_ciclo'].iloc[indice] = cargas_camiones
        ciclos_aljibes['litros_a_camiones'].iloc[indice] = litros_camiones
        ciclos_aljibes['porcentaje_combustible_salida'].iloc[indice] =\
            100* suma_cargas / capacidad_aljibes
        ciclos_aljibes['estado_salida'].iloc[indice] = estado_aljibe
        ciclos_aljibes['eventos_sobre_tiempo'] = eventos_sobre_tiempo      
        
    ciclos_aljibes = ciclos_aljibes.reset_index(drop=True)
    
    # Utilización de los aljibes
    
    # calculo de la columna tiempo en estado de carga de los aljibes
    # en horas
    ciclos_aljibes['tiempo_en_carga'] =\
        ciclos_aljibes['gasto_combustible_ciclo'] / flujo_descarga / 60

    # Se le agrega al tiempo de carga el tiempo que se demora en instalar
    # la manguera y todo    
    ciclos_aljibes['tiempo_en_carga'] = ciclos_aljibes['tiempo_en_carga'] +\
        ciclos_aljibes['tiempo_en_carga']*7/60
    
    ciclos_aljibes['porcentaje_utilizacion_aljibe'] =100 *\
        ciclos_aljibes['tiempo_en_carga'] /\
            ciclos_aljibes['tiempo_ciclo_aljibes']
    
    ciclos_aljibes.drop(columns=['Equipo', 'Prediccion', 'Flota',
                                 'diff hrs', 'Cantidad',
                                 'diff prediccion-real',
                                 'tiempo_entre_cargas'], inplace=True)
    
    ciclos_aljibes = ciclos_aljibes.sort_values(by=['Date'])
    
    
    return ciclos_aljibes


def aplicacion_turno(date):
    """
    Aplica la transformación de Date en el turno al cual pertenece 

    Parameters
    ----------
    date : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    if (date.hour >= 7) & (date.hour < 19):
        turno = "D"
    else:
        turno = "N"
    return turno


def seleccion_temporal_reporte(reporte_abastecimiento_aljibes, fecha):
    """
    Selecciona únicamente los últimos dos turnos, el último turno noche, más 
    el turno día actual    

    Parameters
    ----------
    reporte_abastecimiento_aljibes : Dataframe
        reporte de los ciclos de los aljibes
    fecha : TYPE
        fecha del reporte
    Returns
    -------
    None.

    """
    # dia fecha y hora del día de hoy
    day = fecha.day
    month = fecha.month
    year = fecha.year
    # sacar la fecha del día anterior    
    fecha_anterior = fecha - timedelta(days=1)    
    last_day = fecha_anterior.day
    last_month = fecha_anterior.month
    last_year = fecha_anterior.year    
    # fecha de inicio turno noche
    fecha_inicio_noche = f"{last_day}-{last_month}-{last_year} 19:00:00"
    fecha_inicio_noche = datetime.strptime(fecha_inicio_noche,
                                           "%d-%m-%Y %H:%M:%S")
    # fecha de termino turno dia
    fecha_termino_dia = f"{day}-{month}-{year} 19:00:00"
    fecha_termino_dia = datetime.strptime(fecha_termino_dia,
                                          "%d-%m-%Y %H:%M:%S")
    # Filtrado de solamente los últimos dos turnos
    reporte_abastecimiento_aljibes =\
        reporte_abastecimiento_aljibes[(reporte_abastecimiento_aljibes['Date']\
                                        > fecha_inicio_noche) &
                                       (reporte_abastecimiento_aljibes['Date']\
                                        < fecha_termino_dia)]
    reporte_abastecimiento_aljibes.reset_index(drop=True, inplace=True)
    
    return reporte_abastecimiento_aljibes