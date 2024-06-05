# -*- coding: utf-8 -*-
'''
   Proyecto LEADING 2024
   Script que determina la matriz de energía disponible en baterías a partir del total de energía generada y
   del consumo total
   AUTOR: ISFOC ( fgregorio@isfoc.com; gcalvog@isfoc.com)
'''



### ---- METODO PARA OBTENER EL BALANCE DE ENERGIA EN CADA HORA ----

def balancehorario (generacionTotal, consumoTotal, capacidadDisponibleAnterior):
    
    
    # --------------------------------------------------------------------------
    # Diseño del Algoritmo
    # --------------------------------------------------------------------------


    # Parámetros de entrada

    # Carga inicial de la batería (Q0)     Carga máxima de la batería (Qmax)     Carga mínima de la batería (Qmin)


    # Variables

    # Carga aportada por la bateria (Qaportada)
    # Carga sobrante en batería (Qsob)
    # Generación total: fotovoltaica + eólica (G)
    # Consumo total (C)


    # 1.- Comparamos la generación total en una hora (Gi) con el consumo total en esa hora (Ci)

    #          1.1- Si Gi >= Ci:

    #                 * Qaportada(i) = 0
    #                 * Qsob(i)=Qsob(i-1)+Gi-Ci
    #                   - Si Qsob(i) > Qmax --> Qsob(i)=Qmax & Eexp=Qsob(i)-Qmax & Eimp=0
    #                   - Si Qsob(i) <= Qmax --> Qsob(i)=Qsob(i) & Eexp=0 & Eimp=0


    #          1.2- Si Gi < Ci:

    #               1.2.1- Si Ci-Gi < Qsob(i-1)-Qmin:
    #                       * Qaportada(i) = Ci-Gi
    #                       * Qsob(i)=Qsob(i-1)-Qaportada(i)
    #                       * Eexp=0 & Eimp=0

    #               1.2.2- Si Ci-Gi >= Qsob(i-1)-Qmin:
    #                       * Qaportada(i) = Qsob(i-1)-Qmin
    #                       * Qsob(i)=Qmin
    #                       * Eexp=0 & Eimp=Ci-Gi-Qaportada(i)
    
    

    
    
    if(generacionTotal>= consumoTotal): #Generación mayor que consumo
        capacidadAportada = 0    
        capacidadDisponible = capacidadDisponibleAnterior + generacionTotal - consumoTotal        
        if( capacidadDisponible > limite_capacidad_maxima):   # Si la capacidad disponible excede el limite superior, nos quedamos con el limite superior
            energiaExportada = capacidadDisponible - limite_capacidad_maxima
            capacidadDisponible = limite_capacidad_maxima
            energiaImportada = 0
        else:
            energiaExportada = 0
            energiaImportada = 0
                  
    else: # Consumo mayor que generación
        if ((consumoTotal - generacionTotal) < (capacidadDisponibleAnterior - limite_capacidad_minima)):  # Si la energía puede salir de las baterias sin sobrepasar el límite inferior
            capacidadAportada = consumoTotal - generacionTotal
            capacidadDisponible = capacidadDisponibleAnterior - capacidadAportada
            energiaExportada = 0
            energiaImportada = 0
        else:
            capacidadAportada = capacidadDisponibleAnterior - limite_capacidad_minima
            capacidadDisponible = limite_capacidad_minima
            energiaExportada = 0
            energiaImportada = consumoTotal - generacionTotal - capacidadAportada

                    
    # El balance siempre, independientemente del caso, será la capacidad disponible anterior menos la capacidad disponible
    balance = capacidadDisponibleAnterior - capacidadDisponible
    
    return (balance, capacidadDisponible, capacidadAportada, energiaExportada, energiaImportada)
        
        

### ---- BLOQUE PRINCIPAL DEL PROGRAMA ----    


# Imports para la obtencion de parametros y del sistema
import sys
import datetime
import time
from datetime import date, timedelta

import mysql.connector

import random




# Parametros de BBDD
DB_HOST="127.0.0.1"
DB_USER="mi_ususario"
DB_PASS="0000000"
DB_NAME="datos"

# Variables globales
#id_energy_community_process = 0;
conexion = mysql.connector.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME, charset="latin1")
conexion._open_connection()
cursor = conexion.cursor()


try:
    print (" --- SCRIPT SIMULACION COMPORTAMIENTO BATERIA COMUNIDAD ENERGÉTICA --- ");


    """
    Paso 1: Recuperamos de la base de datos las comunidades para las que hay que hacer la nueva simulacion
    """

    # Consultamos si nos toca ejecutar
    sentenciaObtenerIdComunidad = "SELECT * FROM leading_db.energy_community_process a WHERE ((a.event_id = 30 AND a.result=1000) OR (a.event_id = 35 AND a.result =1001)) AND a.start = (SELECT max(b.start) FROM leading_db.energy_community_process b WHERE a.id_energy_community = b.id_energy_community)";
    
        
    cursor.execute(sentenciaObtenerIdComunidad);
    records = cursor.fetchall()
    #print("Total de comunidades pendientes de procesar:  ", len(records))

    if(len(records)==0):
        # Finalizamos la ejecución
        print("--- NO HAY COMUNIDADES PENDIENTES DE PROCESAR -----")
        cursor.close()
        conexion.commit()
        exit()

    #for row in records:
        #print("id_energy_community_process: ", row[0])
        #print("IdComunidad sin procesar: ", row[1])
        #print("event_id: ", row[2])

    # nos quedamos con uno de los registros random
    itRandom = random.randint(1, len(records))
    #print("itRandom:  ", itRandom)
    # -1 porque arranca el vector 0        
    id_energy_community_process = records[itRandom-1][0]
    idComunidad = records[itRandom-1][1]
    #print ("idComunidad:" + str(idComunidad));

    # Establemos en el energy_community_process la fecha de comienzo

    fcStart = datetime.datetime.now()
    fcStart = fcStart.strftime('%Y-%m-%d %H:%M:%S') 
    sentenciaInsertNuevoRegistroProcess = "INSERT INTO leading_db.energy_community_process (id_energy_community, event_id, start) VALUES ( ";
    sentenciaInsertNuevoRegistroProcess = sentenciaInsertNuevoRegistroProcess + str(idComunidad) + ", 35, '" + fcStart + "') ";
    #print ("sentenciaInsertNuevoRegistroProcess: "+sentenciaInsertNuevoRegistroProcess);
    cursor.execute(sentenciaInsertNuevoRegistroProcess)
    
    

    # FIXME: Se implementa para un único sistema de almacenamiento por CE, habría en un futuro que contemplar la posibilidad de que exista más de un sistema de almacenamiento (el modelo lo soporta)

    # Recuperamos los datos del sistema de almacenamiento correspondiente a esta comunidad
    
    sentenciaObtenerParametrosAlmacenamiento = "SELECT * FROM leading_db.storage_system WHERE id_energy_community = '" + str(idComunidad) + "';";
    #print (sentenciaObtenerParametrosAlmacenamiento)
    
    
    cursor.execute(sentenciaObtenerParametrosAlmacenamiento)
    
    ParametrosAlmacenamiento = cursor.fetchall()
    #print(ParametrosAlmacenamiento)
    
    if (len(ParametrosAlmacenamiento)==0):
            print("--- LA COMUNIDAD NO TIENE SISTEMA DE ALMACENAMIENTO -----")
            now = datetime.datetime.now()
            now = now.strftime('%Y-%m-%d %H:%M:%S') 
            sentenciaUpdate = "UPDATE leading_db.energy_community_process";
            sentenciaUpdate = sentenciaUpdate+ " SET stop = '" + now + "',";
            sentenciaUpdate = sentenciaUpdate+ " result = 1000 ";
            sentenciaUpdate = sentenciaUpdate+ " WHERE id_energy_community = " + str(idComunidad) + " AND event_id = 35 AND start='"+ fcStart + "'";
    
            print ("sentenciaUpdateFinExito: " + sentenciaUpdate);
            cursor.execute(sentenciaUpdate)

            #Cerramos la conexión con la base de datos
            cursor.close()
            conexion.commit();
            exit()
        
    
    id_storage_system = str(ParametrosAlmacenamiento[0][0])    
    limite_capacidad_maxima = float(ParametrosAlmacenamiento[0][6]); #kWh
    limite_capacidad_minima = float(ParametrosAlmacenamiento[0][7]); #kWh
    cargaInicialBateria = float(ParametrosAlmacenamiento[0][8]); #kWh
    
  


    """
    Paso 2: Recuperamos de la base de datos los consumos totales y la generacion total de la comunidad
    """

        
    # Paso 2.1.- Recuperamos un vector con todos las generaciones del año
    
    # Consulta para recuperar la generacion total de la comunidad
    
    sql_aeileading_total_generation_community = """
    SELECT EC.ID_ENERGY_COMMUNITY, generator_data.timestamp, SUM(generator_data.production)
    FROM energy_community AS EC
    INNER JOIN generator AS generator_community ON generator_community.id_energy_community = EC.id_energy_community
    INNER JOIN generator_data AS generator_data ON generator_data.id_generator = generator_community.id_generator
    WHERE EC.ID_ENERGY_COMMUNITY = '""" + str(idComunidad) + """' 
    GROUP BY EC.ID_ENERGY_COMMUNITY, generator_data.timestamp;
    """
    #print (sql_aeileading_total_generation_community)
    
    # Ejecutamos la consulta
    
    cursor.execute(sql_aeileading_total_generation_community)
    registrosDatosGeneracionComunidad = cursor.fetchall();
    


    # Paso 2.2.- Recuperamos un vector con todos los consumos del año

    # Consulta para recuperar el consumo total de la comunidad
    
    sql_aeileading_current_consumption_community = """
    SELECT EC.ID_ENERGY_COMMUNITY, user_data.timestamp, SUM(user_data.consumption)
    FROM energy_community AS EC
    INNER JOIN user AS user_community ON user_community.id_energy_community = EC.id_energy_community
    INNER JOIN user_data AS user_data ON user_data.id_user = user_community.id_user
    WHERE EC.ID_ENERGY_COMMUNITY = '""" + str(idComunidad) + """' 
    GROUP BY EC.ID_ENERGY_COMMUNITY, user_data.timestamp;
    """
    
    #print (sql_aeileading_total_generation_community)
    
    # Ejecutamos la consulta
    
    
    cursor.execute(sql_aeileading_current_consumption_community)
    registrosDatosConsumosComunidad = cursor.fetchall();
    
      
    
    # Paso 2.3.- Deben coincidir el número de horas de generación y consumos
    if(len(registrosDatosConsumosComunidad)!=len(registrosDatosGeneracionComunidad)):
        print("ERROR: No coincide el número de registros de generación respecto a los de consumo")
        exit();
        
    
    
    """
    Paso 3: Creamos una instancia con el estado inicial de la batería
    """
    # Creo el vector donde voy a almacenar los resultados de la simulación de la batería
    
    VectorDatosBaterias = []
    
    
    # Creo la tupla con el estado inicial de la bateria
     
    fecha0= registrosDatosGeneracionComunidad[0][1] - timedelta(hours=1)
    estadoBateria0 = (id_storage_system, fecha0, 0, 0, 0, cargaInicialBateria, 0, 0, 0)
        
    VectorDatosBaterias.append(estadoBateria0)

        

    """
    Paso 4: Creamos una instancia por cada iteración (desde la segunda hora hasta el final)
    """
    for indice in range (0, len(registrosDatosGeneracionComunidad)):

        # Obtengo los datos para la hora
        horaDato = registrosDatosGeneracionComunidad[indice][1];
        horaDato = horaDato.strftime('%Y-%m-%d %H:%M:%S')
        generacionHora = registrosDatosGeneracionComunidad[indice][2];
        consumoHora = registrosDatosConsumosComunidad[indice][2];
        
        
        # Calculo el estado de la bateria en la hora
        estadoBateriaHora = balancehorario(float(generacionHora), float(consumoHora), float(VectorDatosBaterias[indice][5])); 
        
        tuplaVectorDatosBaterias = (id_storage_system, horaDato, float(generacionHora), float(consumoHora), float(estadoBateriaHora[2]), float(estadoBateriaHora[1]), float(estadoBateriaHora[0]), float(estadoBateriaHora[3]), float(estadoBateriaHora[4]))
        
        VectorDatosBaterias.append(tuplaVectorDatosBaterias)
        
        
    
    """
    Paso 5: Imprimimos los resultados / Guardamos en base de datos
    """
    
    # Imprimimos los resultados
    
    #print(VectorDatosBaterias)
    #time.sleep(3)
    
    
    # Guardamos los resultados en la base de datos
    sentenciaInsert = "INSERT INTO `leading_db`.`storage_system_cycle_data` (`id_storage_system`, `timestamp`, `total_energy_generators`, `total_consumption`, `battery_energy_supplied`, `battery_energy_storaged`, `battery_energy_balance`, `community_energy_exported`, `community_energy_imported`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    cursor.executemany(sentenciaInsert, VectorDatosBaterias)
    
    

    """
    Paso 6: Indicamos que la ejecución ha acabado correctamente
    """
    
    now = datetime.datetime.now()
    now = now.strftime('%Y-%m-%d %H:%M:%S') 
    sentenciaUpdate = "UPDATE leading_db.energy_community_process";
    sentenciaUpdate = sentenciaUpdate+ " SET stop = '" + now + "',";
    sentenciaUpdate = sentenciaUpdate+ " result = 1000 ";
    sentenciaUpdate = sentenciaUpdate+ " WHERE id_energy_community = " + str(idComunidad) + " AND event_id = 35 AND start='"+ fcStart + "'";
    
    print ("sentenciaUpdateFinExito: " + sentenciaUpdate);
    cursor.execute(sentenciaUpdate)

    #Cerramos la conexión con la base de datos
    cursor.close()
    conexion.commit();
    



    """
    Paso 7: Si ocurre un error lo indicamos
    """
   

except Exception as ex:

    print ("-----------");
    print ("ERROR: EXCEPCION EN LA EJECUCION DEL PROCESO");
    print (ex);
    print ("-----------");

    # Actulizamos la entrada correspondiente al proceso

    now = datetime.datetime.now()
    now = now.strftime('%Y-%m-%d %H:%M:%S') 
    sentenciaUpdate = "UPDATE leading_db.energy_community_process";
    sentenciaUpdate = sentenciaUpdate+ " SET stop = '" + now + "',";
    sentenciaUpdate = sentenciaUpdate+ " result = 1001 ";
    sentenciaUpdate = sentenciaUpdate+ " WHERE id_energy_community = " + str(idComunidad) + " AND event_id = 35 AND start='"+ fcStart + "'";
    print ("sentenciaUpdateExcepcion: " + sentenciaUpdate);
    cursor.execute(sentenciaUpdate)

    #Cerramos la conexión con la base de datos
    cursor.close()
    conexion.commit(); 


