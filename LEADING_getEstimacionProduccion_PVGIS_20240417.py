'''
   Proyecto LEADING
   Proceso para la obtención de la estimación de la producción fotovoltaica para cada hora, dia y año indicado.
   La salida devuelve en base de datos el promedio para cada hora, independientemente del año (media de 10 años).
   También se obtiene la estimación de la producción eólica para cada hora de cada dia del año indicado.
   La salida devuelve en base de datos el valor del año típico.
   AUTOR: ISFOC ( fgregorio@isfoc.com; gcalvog@isfoc.com)
'''

### ---- METODO PARA OBTENER DATOS DE PV ----

def obtenerDatosPVGIS_PV (pv_peakpower, pv_aspect, pv_angle, lat, lon):
    
    #print("Método obtenerDatosPVGIS_PV");
    
    # Parametros fijos para la obtención de datos
    startyear="2011";
    endyear="2020";
    pvcalculation="1";
    loss="14";
    trackingtype="0";
    browser="0";
    outputformat="json";
    
    jsonRequest = "https://re.jrc.ec.europa.eu/api/v5_2/seriescalc?";
    jsonRequest = jsonRequest + "lat=" + lat + "&";
    jsonRequest = jsonRequest + "lon=" + lon + "&";
    jsonRequest = jsonRequest + "startyear=" + startyear + "&";
    jsonRequest = jsonRequest + "endyear=" + endyear + "&";
    jsonRequest = jsonRequest + "pvcalculation=" + pvcalculation + "&";
    jsonRequest = jsonRequest + "peakpower=" + pv_peakpower + "&";
    jsonRequest = jsonRequest + "loss=" + loss + "&";
    jsonRequest = jsonRequest + "trackingtype=" + trackingtype + "&";
    jsonRequest = jsonRequest + "angle=" + pv_angle + "&";
    jsonRequest = jsonRequest + "aspect=" + pv_aspect + "&";
    jsonRequest = jsonRequest + "browser=" + browser + "&";
    jsonRequest = jsonRequest + "outputformat=" + outputformat;
    
    #print("Cadena compuesta");
    #print(jsonRequest);
    
   
    # Componemos y ejecutamos la petición 
    requestResponse = requests.get(jsonRequest);
    
    # Creamos una matriz de 3 dimensiones para ir guardando los registros recuperados de la petición
    #print ("Creación de la estructura de datos");
 
    matrizResultados = [];
    for dia in range(DIAS):
        matrizResultados.append([])
        for hora in range(HORAS):
            matrizResultados[dia].append([]);
       
    # Procesado de la información. Obtener promedio por hora, para cada día de cada mes (numAños)
    #print ("Inicio procesado de la información y almacenado auxiiar");
    
    ''' Obtenemos el código de respuesta de la petición '''
    requestResponseStatus = requestResponse.status_code;
    
    #print("Codigo de la respuesta obtenido:");
    #print(requestResponseStatus);
    
    if requestResponseStatus==200:
    
        ''' Manejamos la respuesta JSON '''
        json_data = json.loads(requestResponse.text);
    
        json_data_outputHourly = json_data["outputs"]["hourly"];
    
        numResultados = len(json_data_outputHourly);
        #print("numResultados: ");
        #print(numResultados);
    
        # Variable para que coincida la estructura de datos en los años bisiestos
        decrementarBisiesto = False;
       
        for itData in json_data_outputHourly:
        
            # Obtenemos las distintas variables de la petición
            it_time = itData["time"];
            it_year = it_time[0:4];
            it_month = it_time[4:6];
            it_day = it_time[6:8];
            it_hour = it_time[9:11];
                       
            it_energia = itData["P"];
    
            # Almacenamos en la matriz el valor obtenido
            # Obtenemos la primera dimension de nuestra matriz
            dateIT = date(int(it_year), int(it_month), int(it_day));
            dateStartYear = date(int(it_year), 1, 1);
            it_numDayInYear = dateIT - dateStartYear;
            it_numDayInYear = it_numDayInYear.days;
            
            # Almacenamos en la matriz de resultados el valor obtenido
            # FIXME: Ignoramos el 29/02 en años bisiestos para respetar la estructura de datos            
            if int(it_month)==2 and int(it_day)==29:
               decrementarBisiesto = True;
               
            # Si iniciamos anyo limpiamos este flag para que no acumule retraso si no es bisiesto
            if int(it_month)==1 and int(it_day)==1:
               decrementarBisiesto = False;
                   
            #FIXME: Si esto ocurre, el día 29 machaca el 28 y a partir de aqui normal
            if decrementarBisiesto:
                it_numDayInYear = it_numDayInYear -1;
                  
            # Guardamos el dato axiliar obtenido
            matrizResultados [int(it_numDayInYear)][int(it_hour)].append(float(it_energia)); 
        '''
        # Imprimir matriz auxiliar con varios datos en cada hora
        print ("Impresión de los valores almacenados para cada hora");
        for dia in matrizResultados:
            for hora in dia:
                print (hora);  
        '''
        # Calculamos el valor promedio para cada hora y dia alcenado en la matriz auxiliar
        #print ("Calculo promedio de los datos");
    
        # Declaramos la matriz final con 2 dimensiones (dias y horas), donde vamos a almacenar el valor promedio
        matrizEnergiaPromedio = [];
        for dia in range(DIAS):
            matrizEnergiaPromedio.append([])
            for hora in range(HORAS):
                matrizEnergiaPromedio[dia].append(0.0);
    
        # Recorremos la matriz auxiliar con todos los valores para cada hora, calculamos el promedio y almacenamos en la matriz final
    
        for dia in range(DIAS):
            for hora in range(HORAS):
                numRegistrosObtenidosHora = len(matrizResultados[dia][hora]);
                #print(numRegistrosObtenidosHora);
    
                # Variable donde vamos almacenar el promedio a calcular
                valorMedioHoraDia = 0;
                for itRegistro in range (numRegistrosObtenidosHora):
                    valorMedioHoraDia = valorMedioHoraDia + matrizResultados[dia][hora][itRegistro];
                
                valorMedioHoraDia = valorMedioHoraDia / numRegistrosObtenidosHora;
                #print ("obtenerDatosPVGIS_PV_valorMedioHoraDia:",valorMedioHoraDia);
                matrizEnergiaPromedio [dia][hora] = valorMedioHoraDia/1000; # /1000 - Dividiríamos por 1000 si queremos kW
    
        # Devolvemos los datos finales
        #print ("Devolvemos la matriz final con datos promedios de energia por hora y dia");
        
        return matrizEnergiaPromedio;
    
 

### ---- METODO PARA OBTENER DATOS DE EOLICA ----       
    
def obtenerDatosPVGIS_eolica (lat, lon):    
    
    #print("Método obtenerDatosPVGIS_PV");
    
    # Parametros fijos para la obtención de datos
    outputformat="json";

    jsonRequest = "https://re.jrc.ec.europa.eu/api/v5_2/tmy?"; #lo añado para usar el TMY
    
    jsonRequest = jsonRequest + "lat=" + lat + "&";
    jsonRequest = jsonRequest + "lon=" + lon + "&";
    jsonRequest = jsonRequest + "outputformat=" + outputformat;

    #print("Cadena compuesta");
    #print(jsonRequest);
    

    # Componemos y ejecutamos la petición
    requestResponse = requests.get(jsonRequest);
 
    # Creamos una matriz de 3 dimensiones para ir guardando los registros recuperados de la petición
    #print ("Creación de la estructura de datos");
   
    matrizResultados = [];
    
    for dia in range(DIAS):
        matrizResultados.append([])
        for hora in range(HORAS):
            matrizResultados[dia].append([]);
    
  
    # Procesado de la información.
    #print ("Procesado de la información y almacenado auxiiar");
   
    ''' Obtenemos el código de respuesta de la petición '''
    requestResponseStatus = requestResponse.status_code;
    
    #print("Codigo de la respuesta obtenido:");
    #print(requestResponseStatus);

    if requestResponseStatus==200:

        ''' Manejamos la respuesta JSON '''
        json_data = json.loads(requestResponse.text);
    
        json_data_outputHourly = json_data["outputs"]["tmy_hourly"]; #tmy_hourly lo añado para usar el TMY

        numResultados = len(json_data_outputHourly);
        #print("numResultados: ");
        #print(numResultados);

        # Variable para que coincida la estructura de datos en los años bisiestos
        decrementarBisiesto = False;
       
        for itData in json_data_outputHourly:
            
            # Obtenemos las distintas variables de la petición
            it_time = itData["time(UTC)"]; #time(UTC) lo añado para usar el TMY
            it_year = it_time[0:4];
            it_month = it_time[4:6];
            it_day = it_time[6:8];
            it_hour = it_time[9:11];
                       
            it_wind_speed = itData["WS10m"];
            

            # Almacenamos en la matriz el valor obtenido
            # Obtenemos la primera dimension de nuestra matriz
            dateIT = date(int(it_year), int(it_month), int(it_day));
            dateStartYear = date(int(it_year), 1, 1);
            it_numDayInYear = dateIT - dateStartYear;
            it_numDayInYear = it_numDayInYear.days;
            
            # Almacenamos en la matriz de resultados el valor obtenido
            # FIXME: Ignoramos el 29/02 en años bisiestos para respetar la estructura de datos            
            if int(it_month)==2 and int(it_day)==29:
               decrementarBisiesto = True;
               
            # Si iniciamos anyo limpiamos este flag para que no acumule retraso si no es bisiesto
            if int(it_month)==1 and int(it_day)==1:
               decrementarBisiesto = False;
               
            # FIXME: Si esto ocurre, el día 29 machaca el 28 y a partir de aqui normal
            if decrementarBisiesto:
                it_numDayInYear = it_numDayInYear -1;
                

            # Guardamos el dato axiliar obtenido
            matrizResultados [int(it_numDayInYear)][int(it_hour)].append(float(it_wind_speed)); 

        '''
        # Imprimir matriz auxiliar con varios datos en cada hora
        print ("Impresión de los valores almacenados para cada hora");
        for dia in matrizResultados:
            for hora in dia:
                print (hora);  
        '''
                
        # Calculamos el valor promedio para cada hora y dia alcenado en la matriz auxiliar
        #print ("Calculo promedio de los datos");

        # Declaramos la matriz final con 2 dimensiones (dias y horas), donde vamos a almacenar el valor promedio
        matrizGeneracionEolicaPromedio = [];
        for dia in range(DIAS):
            matrizGeneracionEolicaPromedio.append([])
            for hora in range(HORAS):
                matrizGeneracionEolicaPromedio[dia].append(0.0);

        # Recorremos la matriz auxiliar con todos los valores para cada hora, calculamos el promedio y almacenamos en la matriz final

        for dia in range(DIAS):
            for hora in range(HORAS):
                numRegistrosObtenidosHora = len(matrizResultados[dia][hora]);
                #print(numRegistrosObtenidosHora);
                

                # Variable donde vamos a almacenar el promedio a calcular
                valorMedioHoraDia = 0;
                for itRegistro in range (numRegistrosObtenidosHora):
                    valorMedioHoraDia = valorMedioHoraDia + matrizResultados[dia][hora][itRegistro];
                
                                                
                if numRegistrosObtenidosHora != 0: #lo añado para usar el TMY
                    valorMedioHoraDia = valorMedioHoraDia / numRegistrosObtenidosHora; 
                else :   
                    valorMedioHoraDia = valorMedioHoraDia; 
                

                matrizGeneracionEolicaPromedio [dia][hora] = ConvierteVientoEnPotencia(valorMedioHoraDia);

        # Impresión de datos finales
        #print ("Devolvemos la matriz final con datos de energia por hora y dia");
        
        return matrizGeneracionEolicaPromedio
        

### ---- METODO PARA CONVERTIR VIENTO EN POTENCIA ----       
    
def ConvierteVientoEnPotencia(VelocidadViento):
  
    if VelocidadViento < 2:
        PotenciaAerogenerador=0;
    
    elif (VelocidadViento >= 2 and VelocidadViento < 12.5):
        PotenciaAerogenerador=-0.0235*VelocidadViento**6+0.7648*VelocidadViento**5-9.23*VelocidadViento**4+52.365*VelocidadViento**3-118.34*VelocidadViento**2+96.214*VelocidadViento-6.4851;

    elif (VelocidadViento >= 12.5 and VelocidadViento < 16):
        PotenciaAerogenerador=11.446*VelocidadViento**3-520.07*VelocidadViento**2+7715.3*VelocidadViento-34178;

    else:
        PotenciaAerogenerador=3000;
    
    return (PotenciaAerogenerador/1000);  # Lo dividimos por 1000 para dar kW  
    
    
   



### ---- BLOQUE PRINCIPAL DEL PROGRAMA ----    

# Imports para la obtencion de parametros y del sistema
import sys
import datetime
import time
from datetime import date

import mysql.connector
import random

# Import para la realizacion de peticiones
import requests;
import json;

# Parametros de BBDD
DB_HOST="127.0.0.1"
DB_USER="mi_ususario"
DB_PASS="0000000"
DB_NAME="datos"

# Variables globales
DIAS = 365;
HORAS = 24;
id_energy_community_process = 0;
#conexion = mysql.connector.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME)
conexion = mysql.connector.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME, charset="latin1")
conexion._open_connection()
cursor = conexion.cursor()
    
try:

    # Impresión del mensaje de bienvenida
    print (" --- COMIENZO PROCESO OBTENCION ESTIMACION PRODUCCION A TRAVES API PVGIS --- ");


    # Obtenemos el identificador de la comunidad (de base de datos)
    #print ("PASO 0: Obtenemos las comunidades que tienen pendiente simular la generación")
    
    #sentenciaObtenerIdComunidad = "SELECT * FROM leading_db.energy_community_process.result WHERE (event_id = 0) OR (event_id = 10 AND result =1001) ORDER BY id_energy_community_process desc";
    sentenciaObtenerIdComunidad = "SELECT * FROM leading_db.energy_community_process a WHERE ((a.event_id = 0) OR (a.event_id = 10 AND a.result =1001)) AND a.start = (SELECT max(b.start) FROM energy_community_process b WHERE a.id_energy_community = b.id_energy_community)";
    
    cursor.execute(sentenciaObtenerIdComunidad);
    records = cursor.fetchall()
    #print("Total de comunidades pendientes de procesar:  ", len(records))

    if(len(records)==0):
        # Finalizamos la ejecución
        #print("--- NO HAY COMUNIDADES PENDIENTES DE PROCESAR -----");
        cursor.close()
        conexion.commit()
        exit()
            
    #for row in records:
        #print("id_energy_community_process: ", row[0])
        #print("IdComunidad sin procesar: ", row[1])
    
    # nos quedamos con uno de los registros random
    itRandom = random.randint(1, len(records));
    #print("itRandom:  ", itRandom)

    # -1 porque arranca el vector 0        
    id_energy_community_process = records[itRandom-1][0];
    idComunidad = records[itRandom-1][1];
    #print ("idComunidad:" + str(idComunidad));

    # Establemos en el energy_community_process la fecha de comienzo
    fcStart = datetime.datetime.now()
    fcStart = fcStart.strftime('%Y-%m-%d %H:%M:%S')

    sentenciaInsertNuevoRegistroProcess = "INSERT INTO leading_db.energy_community_process (id_energy_community, event_id, start) VALUES ( ";
    sentenciaInsertNuevoRegistroProcess = sentenciaInsertNuevoRegistroProcess + str(idComunidad) + ", 10, '" + fcStart + "') ";
    #print ("sentenciaInsertNuevoRegistroProcess: "+sentenciaInsertNuevoRegistroProcess);
    cursor.execute(sentenciaInsertNuevoRegistroProcess)
    

    # Obtenemos el anyo actual

    currentDateTime = datetime.datetime.now()
    date_year = currentDateTime.date()
    anyoDatosGuardarComunidad = date_year.strftime("%Y")
    #print ("anyoDatosGuardarComunidad:" + str(anyoDatosGuardarComunidad));
    
    # Condicion de anyo bisiesto: el resto tiene que ser 0
    
    resto = int(anyoDatosGuardarComunidad) % 4
    
    
    
    
    #print ("PASO 1: Obtención de los generadores y obtención de la producción con ayuda de la API de PVGIS");
         
    # Consulta para obtener los generadores de la comunidad (de cualquier tipo)
    sql = "SELECT id_generator, id_generator_type, pv_peak_power, pv_module_orientation, pv_module_tilt, latitude, longitude, wind_peak_power  FROM leading_db.generator where id_energy_community = " + str(idComunidad);
    cursor.execute(sql);
    rowDataGeneradores = cursor.fetchall();
    #print("Total de generadores para la comunidad seleccionada:  ", len(rowDataGeneradores))

    if(len(rowDataGeneradores)==0):
        # Finalizamos la ejecución
        #print("--- NO HAY GENERADORES -----");
        exit();
        
    # Para cada generador de la comunidad
    for rowGenerador in rowDataGeneradores:
    
        # Obtenemos todos los datos del generador
        #print ("Obtención de los parámetros de BD para todos los generadores");
        generator_id = str(rowGenerador[0]);
        generator_type = str(rowGenerador[1]);
        pv_peakpower = str(rowGenerador[2]);        
        pv_aspect = str(rowGenerador[3]);
        pv_angle = str(rowGenerador[4]);    
        lat = str(rowGenerador[5]);
        lon = str(rowGenerador[6]);
        wind_peak_power = str(rowGenerador[7]);
        
                
        # --- En función del tipo de generador, llamamos a una u otra función de PVGIS (PV, EOLICO) ---
        
        matrizEnergiaPromedio = [];
        
        if generator_type == '1':
        
            # Cálculo para el tipo de generador FV (generator_type = 1)
            #print("Simulación para generador FV");
            
            matrizEnergiaPromedio = obtenerDatosPVGIS_PV (pv_peakpower, pv_aspect, pv_angle, lat, lon);
            
                   
        elif generator_type == '2':
            
            # Cálculo para el tipo de generador eolico (generator_type = 2)
            #print("Simulación para generador EOLICO")
            
            matrizEnergiaPromedio = obtenerDatosPVGIS_eolica (lat, lon);
        
            
        # Almacenamiento en la base de datos. Componemos el insert y lo ejecutamos
        
        #print ("PASO 2: Almacenamiento de los valores promedios en base de datos");
        try:
            
            # Creamos el vector donde meteremos las tuplas de datos antes de hacer el insert en la base de datos
            
            contadorDias = -1;

            VectorDatosProduccion = []
            
            for dia in range(DIAS):
                #print("dia:",dia);
                contadorDias = contadorDias + 1;
                for hora in range(HORAS):
                    
                    # Obtenemos el valor
                    valorPromedioDiaHora = matrizEnergiaPromedio [dia][hora];
                    #print("valorPromedioDiaHora:",valorPromedioDiaHora);

                    # Obtenemos la fecha para cada dato en función de la posición
                    dateStartYear = date(int(anyoDatosGuardarComunidad), 1, 1);
                    dateIndex = dateStartYear + datetime.timedelta(days=contadorDias)
                    timestampInsert = str(dateIndex);
                    timestampInsert = timestampInsert + " " + str(hora) + ":" + "00:00"; 
                    #print ("timestampInsert: " + str(timestampInsert));

                    # Formamos el vector con las tuplas de datos
                    TuplaDatosProduccion = [str(generator_id), str(timestampInsert), str(valorPromedioDiaHora)]
                    VectorDatosProduccion.append(TuplaDatosProduccion)
           
            
            # Si el año es bisiesto, duplicamos los datos del día 28/02 y los guardamos como si fueran del 29/02
            if resto == 0: # condición para año bisiesto
                #print('Año bisiesto')
                  
                for hora in range(HORAS):

                    # Obtenemos el valor
                    valorPromedioDiaHora = matrizEnergiaPromedio [364][hora];

                    # Obtenemos la fecha para cada dato en función de la posición
                    dateStartYear = date(int(anyoDatosGuardarComunidad), 1, 1);
                    dateIndex = dateStartYear + datetime.timedelta(days=365)
                    timestampInsert = str(dateIndex);
                    timestampInsert = timestampInsert + " " + str(hora) + ":" + "00:00"; 
                    #print ("timestampInsert: " + str(timestampInsert));

                    # Obtenemos la sentencia sql insert
                    sentenciaInsert = "INSERT INTO leading_db.generator_data (id_generator, timestamp, production)  VALUES ( ";
                    sentenciaInsert = sentenciaInsert + str(generator_id) + ", ";
                    sentenciaInsert = sentenciaInsert + "'"+ timestampInsert + "', ";
                    sentenciaInsert = sentenciaInsert + str(valorPromedioDiaHora) + ")";
                    #print (sentenciaInsert);
                    
                    # Ejecutamos la sentencia
                    cursor.execute(sentenciaInsert)
             
            
            # Ejecutamos el insert en base de datos para todos los datos de los generadores que están almacenados en el vector de datos a insertar
            sentenciaInsert = "INSERT INTO leading_db.generator_data (id_generator, timestamp, production)  VALUES (%s, %s, %s)"
            cursor.executemany(sentenciaInsert, VectorDatosProduccion)
             
           
        except Exception as e:
              #conn.rollback()
              print ("-----------");
              print ("ERROR: EXCEPCION EN LA OPERACION DE BASE DE DATOS");
              print (e);
              print ("-----------");
              raise


    # Indicamos que la ejecución ha acabado correctamente   
    
    now = datetime.datetime.now()
    now = now.strftime('%Y-%m-%d %H:%M:%S') 
    sentenciaUpdate = "UPDATE leading_db.energy_community_process";
    sentenciaUpdate = sentenciaUpdate+ " SET stop = '" + now + "',";
    sentenciaUpdate = sentenciaUpdate+ " result = 1000 ";
    sentenciaUpdate = sentenciaUpdate+ " WHERE id_energy_community = " + str(idComunidad) + " AND event_id = 10 AND start='"+ fcStart + "'";
    
    print ("sentenciaUpdateFinExito: " + sentenciaUpdate);
    cursor.execute(sentenciaUpdate)

    # Cerramos la conexión con la base de datos
    cursor.close()
    conexion.commit(); 

# Si ocurre un error lo indicamos
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
    sentenciaUpdate = sentenciaUpdate+ " WHERE id_energy_community = " + str(idComunidad) + " AND event_id = 10 AND start='"+ fcStart + "'";
    print ("sentenciaUpdateExcepcion: " + sentenciaUpdate);
    cursor.execute(sentenciaUpdate)
                    
    # Ejecutamos la sentencia
    cursor.execute(sentenciaUpdate)

    # Cerramos la conexión con la base de datos
    cursor.close()
    conexion.commit(); 
    
    





    
        


    
