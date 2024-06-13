'''
   Proyecto LEADING
   Proceso para crear consumos de usuarios a partir de los perfiles tipo o de los datos reales de consumo almacenados en la base de datos
   AUTOR: ISFOC ( fgregorio@isfoc.com; gcalvog@isfoc.com)
'''



### ---- METODO PARA HACER LA SELECT DE LA BASE DE DATOS QUE EXTRAE EL VALOR DE CONSUMO ----

def select_consumo (id_consumer_profile, mes, dia, hora):
    
    sentenciaObtenerConsumo = "SELECT consumer_profile_consumption.consumption FROM leading_db.consumer_profile_consumption WHERE "
    sentenciaObtenerConsumo = sentenciaObtenerConsumo + "id_consumer_profile = " + str(id_consumer_profile)
    sentenciaObtenerConsumo = sentenciaObtenerConsumo + " AND month = " + str(mes)
    sentenciaObtenerConsumo = sentenciaObtenerConsumo + " AND day = " + str(dia)
    sentenciaObtenerConsumo = sentenciaObtenerConsumo + " AND hour = " + str(hora) + ";"
    
    
    cursor.execute(sentenciaObtenerConsumo);
    registroConsumo = cursor.fetchone()[0]

    return registroConsumo





### ---- METODO PARA OBTENER EL VALOR DEL CONSUMO ADAPTADO ----

def consumoAdaptado (id_consumer_profile, comunidadAutonoma, anyoSimulacion, anyoDatos, mes, dia, hora):
    
    
    # --------------------------------------------------------------------------
    # Pasos a seguir
    # --------------------------------------------------------------------------

    # Parámetros de entrada: año, mes, dia, hora
    # Salida: valor de consumo

    # 1.- De la fecha que se nos ha pasado como argumento y de la misma fecha del año anterior sacamos:
        #     *Año
        #     *Día de la semana    


    # 2.- Miramos si el día que se nos ha pasado por argumento es festivo


    # 3.- Si SI es festivo, miramos si ese día fue festivo el año anterior:
            #          3.1- Si fue festivo, asignamos los mismos consumos horarios que el año anterior
            #          3.2- Si no fue festivo:
            #                 * Busco el domingo de antes de ese día del año anterior
            #                 * Se asignan los consumos del domingo del año anterior


    # 4.- Si NO es festivo, miramos si ese día fue festivo el año anterior:
            #          4.1- Si NO fue festivo, le asignamos los consumos del mismo día de la semana más próximo
            #               al de ese mismo día del año anterior
            #          4.2- Si SI fue festivo:
            #                 * Buscamos el mismo día de la semana que el año pasado fue anterior al festivo
            #                 * Se asignan los consumos de ese día



    # Establecemos la configuración regional en español
    
    #locale.setlocale(locale.LC_TIME, 'es_ES')


    # 1.- Vemos qué día de la semana es la fecha que se nos ha pasado en el año de la simulación y en el año del que se tienen los datos
    fechaAnyoSimulacion = datetime.strptime(f"{anyoSimulacion}-{mes}-{dia}", '%Y-%m-%d')
    dia_semana_fechaAnyoSimulacion = fechaAnyoSimulacion.strftime('%A')
    
    fechaAnyoDatos = datetime.strptime(f"{anyoDatos}-{mes}-{dia}", '%Y-%m-%d')
    dia_semana_fechaAnyoDatos = fechaAnyoDatos.strftime('%A')
    
    
    # Localizamos los festivos del año de la simulación y del año de los datos en la comunidad autónoma de la instalación  
    festivosAnyoSimulacion = holidays.ES(years=int(anyoSimulacion), prov=comunidadAutonoma)
    festivosAnyoDatos = holidays.ES(years=int(anyoDatos), prov=comunidadAutonoma)

    # 2.- Miramos si el día que se nos ha pasado por argumento es festivo el año de la simulacion
    if fechaAnyoSimulacion in festivosAnyoSimulacion:
        # 3.- Si SI es festivo, miramos si ese día fue festivo el año anterior:
        if fechaAnyoDatos in festivosAnyoDatos: 
            # 3.1- Si fue festivo, asignamos los mismos consumos horarios que el año anterior
            mesConsumo = fechaAnyoDatos.month
            diaConsumo = fechaAnyoDatos.day
            horaConsumo = hora
            
            consumo_final = select_consumo (id_consumer_profile, mesConsumo, diaConsumo, horaConsumo)          
            
        else: # 3.2- Si no fue festivo:
            # Busco el domingo más próximo a ese día del año anterior           
            if fechaAnyoDatos.weekday() == 0:
                domingo_mas_cercano = fechaAnyoDatos - timedelta(days=1)
            elif fechaAnyoDatos.weekday() == 1:
                domingo_mas_cercano = fechaAnyoDatos - timedelta(days=2)
            elif fechaAnyoDatos.weekday() == 2:
                domingo_mas_cercano = fechaAnyoDatos - timedelta(days=3)
            else:
                dias_hasta_domingo = 6 - fechaAnyoDatos.weekday()
                domingo_mas_cercano = fechaAnyoDatos + timedelta(days=dias_hasta_domingo)
 
            # Se asignan los consumos del domingo del año anterior
            mesConsumo = domingo_mas_cercano.month
            diaConsumo = domingo_mas_cercano.day
            horaConsumo = hora
            
            consumo_final = select_consumo (id_consumer_profile, mesConsumo, diaConsumo, horaConsumo)          
       
            
    # 4.- Si NO es festivo, miramos si ese día fue festivo el año anterior:
    else:
        # Calculamos la diferencia de dias entre una fecha del año de la simulacion y la misma fecha del año de los datos
        delta = fechaAnyoSimulacion.weekday() - fechaAnyoDatos.weekday()
        if delta < 0:
            nuevo_delta = 7 + delta
        else:
            nuevo_delta = delta
     
        # 4.2- Si SI fue festivo:
        if fechaAnyoDatos in festivosAnyoDatos: 
            # Buscamos el mismo día de la semana que el año pasado fue anterior al festivo
            fechaAnyoDatos = fechaAnyoDatos - timedelta(days=7-nuevo_delta)                  

            # Se asignan los consumos de ese día
            mesConsumo = fechaAnyoDatos.month
            diaConsumo = fechaAnyoDatos.day
            horaConsumo = hora
            
            consumo_final = select_consumo (id_consumer_profile, mesConsumo, diaConsumo, horaConsumo)          
               
        # 4.1- Si NO fue festivo, le asignamos los consumos del mismo día de la semana más próximo al de ese mismo día del año anterior
        else: 
            fechaAnyoDatos = fechaAnyoDatos + timedelta(days=nuevo_delta)
            mesConsumo = fechaAnyoDatos.month
            diaConsumo = fechaAnyoDatos.day
            horaConsumo = hora
            
            consumo_final = select_consumo (id_consumer_profile, mesConsumo, diaConsumo, horaConsumo)          
  
    
    return (consumo_final)



        

### ---- BLOQUE PRINCIPAL DEL PROGRAMA ----    



# Imports para la obtencion de parametros y del sistema
import sys
import time
from datetime import date, datetime, timedelta

import mysql.connector

import random

import holidays

from lecturaIni import lecturaIni

#import locale

# Parametros de BBDD
DB_USER,DB_PASS,DB_HOST,DB_PORT,DB_NAME = lecturaIni()

# Variables globales
id_energy_community_process = 0;
conexion = mysql.connector.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME, charset="latin1")
conexion._open_connection()
cursor = conexion.cursor()

    
try:

    # Impresión del mensaje de bienvenida
    print (" --- COMIENZO PROCESO CREACION DE USUARIOS A PARTIR DE PERFILES --- ");

    # Consultamos si nos toca ejecutar
    #sentenciaObtenerIdComunidad = "SELECT * FROM energy_community_process_result WHERE (event_id = 10 AND result=1000) OR (event_id = 30 AND result=1001) ORDER BY id_energy_community_process desc";
    sentenciaObtenerIdComunidad = "SELECT * FROM leading_db.energy_community_process a WHERE ((a.event_id = 10 AND a.result=1000) OR (a.event_id = 30 AND a.result =1001)) AND a.start = (SELECT max(b.start) FROM leading_db.energy_community_process b WHERE a.id_energy_community = b.id_energy_community)";
    
   
    
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
        #print("event_id: ", row[2])

    # nos quedamos con uno de los registros random
    itRandom = random.randint(1, len(records));
    #print("itRandom:  ", itRandom)
    # -1 porque arranca el vector 0        
    id_energy_community_process = records[itRandom-1][0];
    idComunidad = records[itRandom-1][1];
    #print ("idComunidad:" + str(idComunidad));

    # Establemos en el energy_community_process la fecha de comienzo

    fcStart = datetime.now()
    fcStart = fcStart.strftime('%Y-%m-%d %H:%M:%S')
    sentenciaInsertNuevoRegistroProcess = "INSERT INTO leading_db.energy_community_process (id_energy_community, event_id, start) VALUES ( ";
    sentenciaInsertNuevoRegistroProcess = sentenciaInsertNuevoRegistroProcess + str(idComunidad) + ", 30, '" + fcStart + "') ";
    #print ("sentenciaInsertNuevoRegistroProcess: "+sentenciaInsertNuevoRegistroProcess);
    cursor.execute(sentenciaInsertNuevoRegistroProcess)
    
    # Declaramos el año para el cual crearemos los datos de perfiles (se coge del timestamp)
    
    currentDateTime = datetime.now()
    date_year = currentDateTime.date()
    anyoCargaDatos = date_year.strftime("%Y")
    #print ("anyoCargaDatos:" + str(anyoCargaDatos));


    # Condicion de anyo bisiesto: el resto tiene que ser 0
    
    resto = int(anyoCargaDatos) % 4
    
    # Obtenemos la comunidad autónoma en la que se encuentra la comunidad energética que se quiere simular
    # --------------- FIXME: Hay que sacar las siglas de la comunidad autónoma de la base de datos cuando se cree la nueva tabla ---------------------- 
    comunidadAutonoma = "AR"
   

    # Recuperamos los perfiles de consumo para esta comunidad (`energy_community_consumer_profile`)
    sentenciaObtenerProfiles = "SELECT * FROM leading_db.energy_community_consumer_profile WHERE id_energy_community = " + str(idComunidad)
    cursor.execute(sentenciaObtenerProfiles)
    #print ("Ejecutada consulta de obtención de perfiles:" + str(sentenciaObtenerProfiles));
    registroPerfile = cursor.fetchall()

    # Recorremos cada uno de los perfiles almacenados
    for entradaPerfil in registroPerfile:
        id_energy_community_consumer_profile = entradaPerfil[0]
        id_consumer_profile = entradaPerfil[2]
        #print("VAMOS A PROCESAR UN PERFIL ");
        #print("id_energy_community_consumer_profile: ", id_energy_community_consumer_profile);
        #print("id_consumer_profile: ", id_consumer_profile);
        
        
        # Obtenemos la descripción del id_consumer_profile
        sentenciaObtenerDescripcionPerfil = "SELECT description FROM leading_db.consumer_profile WHERE id_consumer_profile = " + str(id_consumer_profile)
        cursor.execute(sentenciaObtenerDescripcionPerfil)
        registroDescripcionPerfil = cursor.fetchone()
        DescripcionPerfil = registroDescripcionPerfil[0]
        
        
        # Miramos si empieza por "ESxxx"
        if DescripcionPerfil[0] == "E" and DescripcionPerfil[1] == "S":
            
            # Miramos si en la tabla user existe algún usuario con el cups que acabamos de recuperar
            # Si existe, obtenemos el id_user
            try:
                sentenciaObtenerIdUser = "SELECT id_user FROM leading_db.user where cups = '" + str(DescripcionPerfil) + "'"
                cursor.execute(sentenciaObtenerIdUser)
                registroIdUser = cursor.fetchone()
                idNewUser = registroIdUser[0]
                
                # UPDATE table_name
                SentenciaActualizar = "UPDATE leading_db.user "
                # SET column1 = value1, column2 = value2, ...
                SentenciaSet = "SET id_energy_community = " + str(idComunidad)
                # WHERE condition;
                SentenciaWhere = " WHERE id_user = " + str(int(idNewUser)) + ";"
                SentenciaActualizarFinal = SentenciaActualizar + SentenciaSet + SentenciaWhere

                cursor.execute(SentenciaActualizarFinal)

                InsertarDatos = False

            # Si no existe, generamos un nuevo registro en el que añadimos el cups del perfil
            except TypeError as e:
                # Obtenemos de la secuencia de usuarios el id del usuario que se creará
                selectSecuenciaUser = "SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'leading_db' AND TABLE_NAME = 'user';";
                #print ("selectSecuenciaUser: " + str(selectSecuenciaUser));
                cursor.execute(selectSecuenciaUser)
                idNewUser = cursor.fetchone()[0];
                #print("Id del nuevo usuario a crear devuelto por la secuencia de la tabla de usuarios:", idNewUser);

                # Creamos un nuevo usuario en base de datos
                sentenciaInsertNuevoUsuario = "INSERT INTO leading_db.user (`id_energy_community`, `name`, `surname1`, `surname2`,  `nif`, `address`, `zip`, `tel`, `email`, `cups`, `energy_poverty_beneficiary`, `power_tariff_1`, `power_tariff_2`, `energy_tariff_p1`, `energy_tariff_p2`, `energy_tariff_p3`, `exp_energy_tariff`) VALUES ( ";
                sentenciaInsertNuevoUsuario = sentenciaInsertNuevoUsuario + str(idComunidad) + ", ";
                sentenciaInsertNuevoUsuario = sentenciaInsertNuevoUsuario + " 'GEN_AUT', 'GEN_AUT', 'GEN_AUT', 'GEN_AUT', 'GEN_AUT', 'GEN_AUT', 'GEN_AUT', 'GEN_AUT', '" + str(DescripcionPerfil) + "', 0, 0, 0, 0, 0, 0, 0);";
                #print ("sentenciaInsertNuevoUsuario: " + str(sentenciaInsertNuevoUsuario))
                cursor.execute(sentenciaInsertNuevoUsuario)
                
                InsertarDatos = True

        else:
            # Obtenemos de la secuencia de usuarios el id del usuario que se creará
            selectSecuenciaUser = "SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'leading_db' AND TABLE_NAME = 'user';";
            #print ("selectSecuenciaUser: " + str(selectSecuenciaUser));
            cursor.execute(selectSecuenciaUser)
            idNewUser = cursor.fetchone()[0];
            #print("Id del nuevo usuario a crear devuelto por la secuencia de la tabla de usuarios:", idNewUser);

            # Creamos un nuevo usuario en base de datos
            sentenciaInsertNuevoUsuario = "INSERT INTO leading_db.user (`id_energy_community`, `name`, `surname1`, `surname2`,  `nif`, `address`, `zip`, `tel`, `email`, `cups`, `energy_poverty_beneficiary`, `power_tariff_1`, `power_tariff_2`, `energy_tariff_p1`, `energy_tariff_p2`, `energy_tariff_p3`, `exp_energy_tariff`) VALUES ( ";
            sentenciaInsertNuevoUsuario = sentenciaInsertNuevoUsuario + str(idComunidad) + ", ";
            sentenciaInsertNuevoUsuario = sentenciaInsertNuevoUsuario + " 'GEN_AUT', 'GEN_AUT', 'GEN_AUT', 'GEN_AUT', 'GEN_AUT', 'GEN_AUT', 'GEN_AUT', 'GEN_AUT', " + str(idNewUser) + ", 0, 0, 0, 0, 0, 0, 0);";
            #print ("sentenciaInsertNuevoUsuario: " + str(sentenciaInsertNuevoUsuario));
            cursor.execute(sentenciaInsertNuevoUsuario)

            InsertarDatos = True

        
        # Obtenemos los datos del perfil y los asociamos al nuevo usuario
        sentenciaObtenerPerfilUsuario = "SELECT consumer_profile_consumption.id_consumer_profile_consumption, consumer_profile_consumption.id_consumer_profile, consumer_profile_consumption.year, consumer_profile_consumption.month, consumer_profile_consumption.day, consumer_profile_consumption.hour, consumer_profile_consumption.consumption FROM leading_db.consumer_profile_consumption WHERE id_consumer_profile = " + str(id_consumer_profile);
        cursor.execute(sentenciaObtenerPerfilUsuario);
        #print ("Ejecutada la consulta para obtener los datos del perfil del usuario:" + str(sentenciaObtenerPerfilUsuario));
        registroDatosPerfilUsuario = cursor.fetchall()


        # Recorremos cada uno de los datos correspondientes al perfil y los almacenamos en un vector de tuplas de datos
        VectorDatosConsumo = []

        if InsertarDatos:
        
            for datoPerfilUsario in registroDatosPerfilUsuario:
                anyo = datoPerfilUsario[2]
                anyo = int(anyo)
                mes = datoPerfilUsario[3]
                mes = int(mes);
                dia = datoPerfilUsario[4]
                dia = int(dia);
                hora = datoPerfilUsario[5]
                hora = int(hora);


                if anyo != 0:  # Si tenemos datos reales (el año de la tabla consumer_profile_consumption es distinto de 0)
                    if mes == 2 and dia ==29: # significa que tenemos datos reales de un año    bisiesto                        
                        pass
                    else:
                        # Llamamos a la función que nos devuelve un consumo para la fecha indicada a partir de los datos    del año anterior y teniendo en cuenta festivos y fines de semana
                        valorConsumo = consumoAdaptado (id_consumer_profile, comunidadAutonoma, anyoCargaDatos, anyo, mes,  dia, hora)

                        # Componemos la fecha a insertar
                        dateConsumo = date(int(anyoCargaDatos), mes, dia);
                        dateConsumo = str(dateConsumo) + " " + str(hora) + ":" + "00:00";

                        #Componemos el vector de datos a insertar
                        TuplaVectorDatosConsumo = [str(idNewUser),str(dateConsumo),str(valorConsumo),0,0,0]
                        #print(TuplaVectorDatosConsumo)
                        VectorDatosConsumo.append(TuplaVectorDatosConsumo)
                        #print(VectorDatosConsumo)


                else:
                    valorConsumo = datoPerfilUsario[6]

                    # Componemos la fecha a insertar
                    dateConsumo = date(int(anyoCargaDatos), mes, dia);
                    dateConsumo = str(dateConsumo) + " " + str(hora) + ":" + "00:00";


                    #Componemos el vector de datos a insertar
                    TuplaVectorDatosConsumo = [str(idNewUser),str(dateConsumo),str(valorConsumo),0,0,0]
                    #print(TuplaVectorDatosConsumo)
                    VectorDatosConsumo.append(TuplaVectorDatosConsumo)
                    #print(VectorDatosConsumo)        


                # Si el año es bisiesto, duplicamos los datos del día 28/02 en el día 29/02

                if resto==0 and mes==2 and dia==28:
                    #print('Año bisiesto. Añadimos un nuevo registro para el día 29 de febrero.')
                    # Componemos la fecha a insertar
                    dateConsumo = date(int(anyoCargaDatos), 2, 29);
                    dateConsumo = str(dateConsumo) + " " + str(hora) + ":" + "00:00";
                    #print ("Fecha del dato: " + str(dateConsumo));

                    # Ejecutamos el insert en base de datos para cada nuevo dato del nuevo usuario

                    sentenciaInsertNuevoDatoPerfil = "INSERT INTO leading_db.user_data (`id_user`, `timestamp`,     `consumption`, `partition_coefficient`, `partition_energy`, `partition_surplus_energy`) VALUES( ";
                    sentenciaInsertNuevoDatoPerfil = sentenciaInsertNuevoDatoPerfil + str(idNewUser) + ", ";
                    sentenciaInsertNuevoDatoPerfil = sentenciaInsertNuevoDatoPerfil + "'" + str(dateConsumo) + "', ";
                    sentenciaInsertNuevoDatoPerfil = sentenciaInsertNuevoDatoPerfil + str(valorConsumo) + ", ";
                    sentenciaInsertNuevoDatoPerfil = sentenciaInsertNuevoDatoPerfil + " 0, 0, 0); ";
                    #print ("sentenciaInsertNuevoDatoPerfil: " + str(sentenciaInsertNuevoDatoPerfil));  
                    cursor.execute(sentenciaInsertNuevoDatoPerfil)

        
            # Ejecutamos el insert en base de datos para todos los datos del nuevo usuario que están almacenados en el vector de datos a insertar
            #print('Hacemos el EXECUTEMANY')
            sentenciaInsertNuevoDatoPerfil = "INSERT INTO leading_db.user_data (`id_user`, `timestamp`, `consumption`, `partition_coefficient`, `partition_energy`, `partition_surplus_energy`) VALUES(%s, %s, %s, %s, %s, %s)";
            #print(sentenciaInsertNuevoDatoPerfil)
            cursor.executemany(sentenciaInsertNuevoDatoPerfil, VectorDatosConsumo)
            #print('EXECUTEMANY ejecutado')
        
        
  
    # Indicamos que la ejecución ha acabado correctamente

    now = datetime.now()
    now = now.strftime('%Y-%m-%d %H:%M:%S') 
    sentenciaUpdate = "UPDATE leading_db.energy_community_process";
    sentenciaUpdate = sentenciaUpdate+ " SET stop = '" + now + "',";
    sentenciaUpdate = sentenciaUpdate+ " result = 1000 ";
    sentenciaUpdate = sentenciaUpdate+ " WHERE id_energy_community = " + str(idComunidad) + " AND event_id = 30 AND start='"+ fcStart + "'";
    
       
    print ("sentenciaUpdateFinExito: " + sentenciaUpdate);
    cursor.execute(sentenciaUpdate)

    #Cerramos la conexión con la base de datos
    cursor.close()
    conexion.commit();
    
### PASO 8: Si ocurre un error lo indicamos
except Exception as ex:

    print ("-----------");
    print ("ERROR: EXCEPCION EN LA EJECUCION DEL PROCESO");
    print (ex);
    print ("-----------");

    # Actulizamos la entrada correspondiente al proceso

    now = datetime.now()
    now = now.strftime('%Y-%m-%d %H:%M:%S') 
    sentenciaUpdate = "UPDATE leading_db.energy_community_process";
    sentenciaUpdate = sentenciaUpdate+ " SET stop = '" + now + "',";
    sentenciaUpdate = sentenciaUpdate+ " result = 1001 ";
    sentenciaUpdate = sentenciaUpdate+ " WHERE id_energy_community = " + str(idComunidad) + " AND event_id = 30 AND start='"+ fcStart + "'";
   
    print ("sentenciaUpdateExcepcion: " + sentenciaUpdate);
    cursor.execute(sentenciaUpdate)

    #Cerramos la conexión con la base de datos
    cursor.close()
    conexion.commit(); 
