from DTOs.ComunidadEnergeticaDTO import ComunidadEnergeticaDTO
from DTOs.DatoConsumoUsuarioDTO import DatoConsumoUsuarioDTO
from DTOs.GeneradorEnergiaDTO import GeneradorEnergiaDTO
from DTOs.UsuarioDTO import UsuarioDTO

import datetime
import numpy as np
import logging



def obtenerDatosComunidadEnergeticaDesdeBBDD (agenteEjecucionMySql, idComunidadEnergetica:str, fcSimulacionDesde:str, fcSimulacionHasta:str):
    """
        author: fdgregorio
        
        traductor: jnaveiro
    
        Definición: Método encargado de obtener los distintos datos de la comunidad energética, incluyendo los diversos usuarios y generadores.
        
        Variables de entrada: agenteEjecucionMySql, idComunidadEnergetica(str), fcSimulacionDesde(str), fcSimulacionHasta(str)
        
        Variables de salida: comunidadEnergeticaReturned(ComunidadEnergeticaDTO)
        
        Objetos relevantes:
        
            1. comunidadEnergeticaReturned
            2. agenteEjecucionMySql
        
        Librerías que se utilizan:
        
            1. Simulador.DTOs.ComunidadEnergeticaDTO
            2. Simulador.DTOs.DatoConsumoUsuarioDTO
            3. Simulador.DTOs.GeneradorEnergiaDTO
            4. Simulador.DTOs.UsuarioDTO
            5. logging
    """
    try :

        # Declaramos el listado a devolver
        comunidadEnergeticaReturned = ComunidadEnergeticaDTO()

        # Paso 1: Obtenemos los datos relativos a la comunidad energética
        sqlDatosGeneralesCE = "SELECT CE.id_energy_community, CE.name, CE.min_participation, CE.max_participation, CE.energy_poverty " + " FROM leading_db.energy_community as CE" + " WHERE CE.id_energy_community = " + idComunidadEnergetica
        rs_comunidad = agenteEjecucionMySql.ejecutar(sqlDatosGeneralesCE)

        # Devolvemos los datos tratados
        for rs in rs_comunidad:

            # Declaramos el objeto a devolver en el listado
            comunidadEnergeticaReturned.setIdComunidadEnergetica(str(rs[0]))
            comunidadEnergeticaReturned.setDsComunidadEnergetica(str(rs[1]))
            comunidadEnergeticaReturned.setCuotaParticipacion_min(float(rs[2]))
            comunidadEnergeticaReturned.setCuotaParticipacion_max(float(rs[3]))
            comunidadEnergeticaReturned.setPorcentajeDedicadoPobrezaEnergetica(float(rs[4]))

            # Añadimos objeto relleno al listado
            # listComunidadEnergetica.add(comunidadEnergetica)

        # Paso 2: Obtenemos los datos relativos a los generadores de la comunidad
        sqlDatosGeneradorComunidad = "SELECT gen.id_generator, gen.description " + " FROM leading_db.generator as gen " + " WHERE gen.id_energy_community = " + idComunidadEnergetica
        rs_generadores = agenteEjecucionMySql.ejecutar(sqlDatosGeneradorComunidad)
        # Devolvemos los datos tratados
        for rs in rs_generadores:

            # Declaramos el objeto a devolver en el listado
            genEnergia = GeneradorEnergiaDTO()
            genEnergia.setIdGeneradorEnergia(str(rs[0]))
            genEnergia.setDsGeneradorEnergia(str(rs[1]))

            # Por cada generador los cargamos sus datos
            # private double consumos [][] = new double [3][7]
            sqlDatosGeneracionGenerador = "SELECT gen_data.timestamp, DAYOFYEAR (gen_data.timestamp) -1 as numDayYear, HOUR (gen_data.timestamp) as numHoraDia , gen_data.production " + "FROM leading_db.generator_data gen_data " + " WHERE gen_data.id_generator = " + genEnergia.getIdGeneradorEnergia() + " AND gen_data.timestamp>='" + fcSimulacionDesde + "' " + " AND gen_data.timestamp<='" + fcSimulacionHasta + "' "

            rs_generaciones_generador = agenteEjecucionMySql.ejecutar(sqlDatosGeneracionGenerador)
            # Devolvemos los datos tratados
            for rs2 in rs_generaciones_generador:

                # String fechaConsumo = (rs_consumos_usuarios.getString(1))
                iNumDiaConsumo = int(rs2[1])
                iNumHoraConsumo = int(rs2[2])
                production = float(rs2[3])

                genEnergia.getGeneracion()[iNumDiaConsumo][iNumHoraConsumo] = production

            # Añadimos objeto relleno al listado
            comunidadEnergeticaReturned.generadoresComunidad.append(genEnergia)
            
        # FIXME: Pendiente de validar, Paso 2.5: Estado del balance de las baterías
        battery = False
        sqlDatosBateriasComunidad = "SELECT batt.id_storage_system, batt.ds_storage_system " + " FROM leading_db.storage_system as batt " + " WHERE batt.id_energy_community = " + idComunidadEnergetica
        # Consulta si hay baterías de la hoja storage_system
        rs_baterias = agenteEjecucionMySql.ejecutar(sqlDatosBateriasComunidad)
        if len(rs_baterias)>0:
            battery = True

        if battery:
            # Recuperar, en caso de que haya baterías, los datos de la columna balance de la hoja   storage_system_cycle_data
            for rs in rs_baterias:
            # Declaramos el objeto a devolver en el listado
                batEnergia = GeneradorEnergiaDTO()
                batEnergia.setIdGeneradorEnergia(str(rs[0]))
                batEnergia.setDsGeneradorEnergia(str(rs[1]))
                
                #columna battery_energy_balance
                sqlDatosGeneracionBateria = "SELECT gen_data.timestamp, DAYOFYEAR (gen_data.timestamp) -1 as numDayYear, HOUR (gen_data.timestamp) as numHoraDia , gen_data.battery_energy_balance " + "FROM leading_db.storage_system_cycle_data as gen_data " + " WHERE gen_data.id_storage_system = " + batEnergia.getIdGeneradorEnergia() + " AND gen_data.timestamp>='" + fcSimulacionDesde + "' " + " AND   gen_data.timestamp<='" + fcSimulacionHasta + "' "

                rs_generaciones_bateria = agenteEjecucionMySql.ejecutar(sqlDatosGeneracionBateria)
                # Devolvemos los datos tratados
                for rs2 in rs_generaciones_bateria:

                    # String fechaConsumo = (rs_consumos_usuarios.getString(1))
                    iNumDiaConsumo = int(rs2[1])
                    iNumHoraConsumo = int(rs2[2])
                    production = float(rs2[3])

                    # Sumarlo a los generadores
                    batEnergia.getGeneracion()[iNumDiaConsumo][iNumHoraConsumo] = production

                # Añadimos objeto relleno al listado
                comunidadEnergeticaReturned.generadoresComunidad.append(batEnergia)


        # Paso 3: Obtenemos los datos relativos a los usuarios de la comunidad
        sqlDatosUsuariosComunidad = "SELECT users.id_user, users.cups, CONCAT(users.name, ' ', users.surname1, ' ', users.surname2) as nombreCompleto "+ " FROM leading_db.user as users " + " where users.id_energy_community = " + idComunidadEnergetica

        rs_usuarios = agenteEjecucionMySql.ejecutar(sqlDatosUsuariosComunidad)
        # Devolvemos los datos tratados
        for rs in rs_usuarios:

            # Declaramos el objeto a devolver en el listado
            usuarioDTO = UsuarioDTO()
            usuarioDTO.setIdUsuario(str(rs[0]))
            usuarioDTO.setCupsUsuario(str(rs[1]))
            usuarioDTO.setDsUsuario(str(rs[2]))

            # Por cada usuario cargamos su lista de consumos
            # private double consumos [][] = new double [3][7]
            sqlConsumosUsuariosComunidad = "SELECT user_data.id_user_data, user_data.timestamp, DAYOFYEAR (user_data.timestamp) -1 as numDayYear, HOUR (user_data.timestamp) as numHoraDia , user_data.consumption, user_data.partition_coefficient, user_data.partition_energy, user_data.partition_surplus_energy " + "FROM leading_db.user_data as user_data " + "WHERE user_data.id_user = " + usuarioDTO.getIdUsuario() + " AND user_data.timestamp>='" + fcSimulacionDesde + "' " + " AND user_data.timestamp<='" + fcSimulacionHasta + "' "

            rs_consumos_usuarios = agenteEjecucionMySql.ejecutar(sqlConsumosUsuariosComunidad)
            # Devolvemos los datos tratados
            for rs2 in rs_consumos_usuarios:

                idUserData = int(rs2[0])
                fechaConsumo = str(rs2[1])
                iNumDiaConsumo = int(rs2[2])
                iNumHoraConsumo = int(rs2[3])
                energiaConsumidaUsuarioDiaHora = float(rs2[4])

                # Resto de variables aqui no hay que cargarlas (son null)
                datoConsumoUsuarioDTO = DatoConsumoUsuarioDTO()
                datoConsumoUsuarioDTO.setIdUserData(idUserData)
                datoConsumoUsuarioDTO.setFcDatoConsumoHorario(fechaConsumo)
                datoConsumoUsuarioDTO.setValorDatoConsumoHorario(energiaConsumidaUsuarioDiaHora)

                usuarioDTO.consumos[iNumDiaConsumo][iNumHoraConsumo] = datoConsumoUsuarioDTO

            
            # Añadimos objeto relleno al listado
            comunidadEnergeticaReturned.getUsuariosComunidad().append(usuarioDTO)
        

        # Devolvemos la lista rellena
        return comunidadEnergeticaReturned

    except Exception as e :
        mensajeExcepServ = "Error en el servicio: ComunidadesEnergeticasServicio.obtenerDatosComunidadEnergeticaDesdeBBDD "
        # print(mensajeExcepServ+"{}, {}".format(e,type(e)))
        logging.error(mensajeExcepServ)
        logging.exception("Unexpected {}, {}".format(e,type(e)))

        raise SystemExit(e)

def almacenarDatosCalculadosTxt(ce,anyo):
    """
        author: fdgregorio
        
        traductor: jnaveiro
    
        Definición: Metodo encargado de alamcenar en base de datos todos los cálculos realizados durante la simulación junto al dato del consumo del usuario de la comunidad.
        
        Variables de entrada: agenteEjecucionMySql, ce(ComunidadEnergeticaDTO)
        
        Variables de salida: Ninguno
        
        Objetos relevantes:
        
            1. ce
            2. agenteEjecucionMySql
        
        Librerías que se utilizan:
        
            1. Simulador.DTOs.ComunidadEnergeticaDTO
            2. datetime
            3. logging
    """
    try:
        # Mensaje de log al inicio del método
        logging.info(datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S') + " --> ComunidadesEnergeticasServicio.almacenarDatosCalculadosTxt:   Inicio de la ejecución del método")
        # Recorremos los distintos clientes de la comunidad
        with open("CAU"+str(ce.getIdComunidadEnergetica())+"_"+str(anyo)+".txt","w") as f:
            fecha0 = datetime.datetime(year=anyo,month=1,day=1,hour=0,minute=0,second=1)
            for itUsuario in range(len(ce.getUsuariosComunidad())):
                horasSimul = 0
                fecha = datetime.datetime(year=anyo,month=1,day=1,hour=0,minute=0,second=1)
                consumosUsuarioIt = ce.getUsuariosComunidad()[itUsuario].getConsumos()
                numDias = len(consumosUsuarioIt)
                CUPS = ce.getUsuariosComunidad()[itUsuario].getCupsUsuario()

                for itDiaConsumo in range(numDias):
                    numHoras = len(consumosUsuarioIt[itDiaConsumo])
                    # if (fecha.month!=2 or fecha.day!=29):
                    for itHoraConsumo in range(numHoras):
                        coeficienteReparto = ce.getUsuariosComunidad()[itUsuario].getCoeficientesReparto()[itDiaConsumo][itHoraConsumo]
                        horasSimul += 1
                        f.write("\n{:22s};{:4s};{:.6f}".format(CUPS[-22:],str(10000+horasSimul)[-4:],coeficienteReparto/100))
                    
                    fecha += datetime.timedelta(days=1)

        # Elimina la primera linea en blanco
        g = open("CAU"+str(ce.getIdComunidadEnergetica())+"_"+str(anyo)+".txt","r")
        todo = g.readlines()
        g.close()
        with open("CAU"+str(ce.getIdComunidadEnergetica())+"_"+str(anyo)+".txt","w") as g:
            g.writelines(todo[1:])

        # Mensaje de log al inicio del método
        logging.info(datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S') + " --> ComunidadesEnergeticasServicio.almacenarDatosCalculadosComunidadEnergetica: Fin de la ejecución del método")

        return "CAU"+str(ce.getIdComunidadEnergetica())+"_"+str(anyo)+".txt"

    except Exception as e:
        raise SystemExit(e)

def almacenarDatosCalculadosComunidadEnergetica(agenteEjecucionMySql, ce):
    """
        author: fdgregorio
        
        traductor: jnaveiro
    
        Definición: Metodo encargado de alamcenar en base de datos todos los cálculos realizados durante la simulación junto al dato del consumo del usuario de la comunidad.
        
        Variables de entrada: agenteEjecucionMySql, ce(ComunidadEnergeticaDTO)
        
        Variables de salida: Ninguno
        
        Objetos relevantes:
        
            1. ce
            2. agenteEjecucionMySql
        
        Librerías que se utilizan:
        
            1. Simulador.DTOs.ComunidadEnergeticaDTO
            2. datetime
            3. logging
    """
    try:
        #sqlTruncaTabla = "TRUNCATE TABLE leading_db.resultados;"
        #agenteEjecucionMySql.ejecutar(sqlTruncaTabla)

        # Mensaje de log al inicio del método
        logging.info(datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S') + " --> ComunidadesEnergeticasServicio.almacenarDatosCalculadosComunidadEnergetica:   Inicio de la ejecucion del metodo")
        print("Almacenamiento de los datos de la comunidad energetica: ",ce.idComunidadEnergetica)
        # Recorremos los distintos clientes de la comunidad
        for itUsuario in range(len(ce.getUsuariosComunidad())):

            consumosUsuarioIt = ce.getUsuariosComunidad()[itUsuario].getConsumos()
            numDias = len(consumosUsuarioIt)
            id_user = ce.getUsuariosComunidad()[itUsuario].getIdUsuario()

            #Actualizar consumos
            # sqlUpdateConsumos = "UPDATE leading_db.user_data SET partition_coefficient = %s, partition_energy = %s , partition_surplus_energy = %s WHERE id_user_data = %s ; "
            sqlUpdateConsumos = " INSERT INTO leading_db.resultados (fecha,id_user, consumption, partition_coefficient, partition_energy, partition_surplus_energy) VALUES (%s, %s, %s, %s, %s, %s);"
            listaInfo = []
            
            for itDiaConsumo in range(numDias):
                numHoras = len(consumosUsuarioIt[itDiaConsumo])
                for itHoraConsumo in range(numHoras):
                    coeficienteReparto = ce.getUsuariosComunidad()[itUsuario].getCoeficientesReparto()[itDiaConsumo][itHoraConsumo]
                    energiaRepartida = ce.getUsuariosComunidad()[itUsuario].getEnergiaReparto()[itDiaConsumo][itHoraConsumo]
                    energiaExcedente = ce.getUsuariosComunidad()[itUsuario].getEnergiaReparto_excedentes()[itDiaConsumo][itHoraConsumo]

                    # Sólo si el día y la hora que en base de datos tienen consumos
                    if (consumosUsuarioIt[itDiaConsumo][itHoraConsumo] != None) :
                        # CODIGO OPTIMIZADO CON PK (da más o menos igual de rendimiento)                        
                        fechaAux = consumosUsuarioIt[itDiaConsumo][itHoraConsumo].getFcDatoConsumoHorario()
                        consumoAux = consumosUsuarioIt[itDiaConsumo][itHoraConsumo].getValorDatoConsumoHorario()
                        tuplaAux = (str(fechaAux),str(id_user),str(consumoAux),str(coeficienteReparto),str(energiaRepartida),str(energiaExcedente))
                        
                        listaInfo.append(tuplaAux)
                        # agenteEjecucionMySql.ejecutar(sqlUpdateConsumos,tuplaAux)
                        # agenteEjecucionMySql.commitTransaction()

            # comando = ""
            # for i in listaInfo:
            #     comando += i
            agenteEjecucionMySql.ejecutarMuchos(sqlUpdateConsumos,listaInfo)
            
            # pocos = "SELECT * FROM leading_db.user_data WHERE id_user_data = " + str(idUserData)
            # print(agenteEjecucionMySql.ejecutar(pocos))
            print("Se ha realizado la actualización del ususario: ",id_user)
        # Mensaje de log al inicio del método
        logging.info(datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S') + " --> ComunidadesEnergeticasServicio.almacenarDatosCalculadosComunidadEnergetica: Fin de la ejecución del método")
    except Exception as e:
        raise SystemExit(e)

def obtenerParametrosEjecucionSimulacion(agenteEjecucionMySql,anyo):
    """
        author: fdgregorio
        
        traductor: jnaveiro
    
        Definición: Método encargado de obtener lso parametros de ejecución de base de datos y actualizar la fecha de comienzo de la ejecución de la misma
        
        Variables de entrada: agenteEjecucionMySql,anyo(int)
        
        Variables de salida: stringToResult(list)
        
        Objetos relevantes: Ninguno
        
        Librerías que se utilizan:
        
            1. numpy
            2. datetime
            3. logging
    """
    try:
        # Mensaje de log al inicio del método
        logging.info(datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S') + " --> ComunidadesEnergeticasServicio.obtenerParametrosEjecucionSimulacion: Inicio   de la ejecución del método")

        # Por cada usuario cargamos su lista de consumos
        # private double consumos [][] = new double [3][7]
        # Consultamos si nos toca ejecutar
        sqlCommunityProcess = "SELECT * FROM leading_db.energy_community_process a WHERE ((a.event_id = 35 AND a.result=1000) OR (a.event_id = 40 AND a.result =1001)) AND a.start = (SELECT max(b.start) FROM leading_db.energy_community_process b WHERE a.id_energy_community = b.id_energy_community)"
        # sqlCommunityProcess = "SELECT * FROM leading_db.energy_community_process WHERE (event_id = 35 AND result =1000) OR (event_id = 40 AND result =1001) ORDER BY id_energy_community_process desc"

        rs_communityProcess = agenteEjecucionMySql.ejecutar(sqlCommunityProcess)

        # Declaramos las variables a rellenar
        idEnergyCommunityProcess = ""
        idEnergyCommunity = ""
        simulacion_fcDesde = ""
        simulacion_fcHasta = ""

        # Rellenamos las variables con el resultado de la consulta
        vector_idEnergyCommunityProcess = []
        vector_idEnergyCommunity = []
        contadorResultados = 0
        for rs in rs_communityProcess:
            vector_idEnergyCommunityProcess.append(str(rs[0]))
            vector_idEnergyCommunity.append(str(rs[1]))
            contadorResultados = contadorResultados + 1

        logging.info("Número de simulaciones pendientes: " + str(contadorResultados))

        if contadorResultados == 0:
            return None

        # Obtenemos un ramdom para el numero de resultados
        # int nRandom = (int) (Math.random() * ((contadorResultados+1) - 1)) + 1
        if contadorResultados>1:
            nRandom = np.random.randint(0,(contadorResultados - 1))
        else:
            nRandom = 0
        #Para hacer pruebas con una comunidad concreta, quitar el comentario y poner el número en orden en el que aparece en la lista vector_idEnergyCommunity
        # nRandom = 0
        logging.info("Random seleccionado: " + str(nRandom))

        # Nos quedamos con el seleccionado por el random
        idEnergyCommunityProcess = vector_idEnergyCommunityProcess[nRandom]
        idEnergyCommunity = vector_idEnergyCommunity[nRandom]
        logging.info("idEnergyCommunity: " + str(idEnergyCommunity))

        # # Fecha comienzo y de fin
        logging.info("Anyo de la simulación: " + str(anyo))

        # DateFormat sdfMYSQL = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        sFechaActual = datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S')

        # sqlUpdateConsumos = "UPDATE leading_db.energy_community_process " + "SET start = '" + sFechaActual + "'" + "WHERE   id_energy_community_process = " + idEnergyCommunityProcess
        sqlUpdateConsumos = "INSERT INTO leading_db.energy_community_process (id_energy_community, event_id, start) VALUES ( " + str(idEnergyCommunity) + ", 40, '" + sFechaActual + "') "
        # sqlUpdateConsumos = "UPDATE leading_db.energy_community_process " + "SET start = '" + sFechaActual + "'" + "WHERE   id_energy_community_process = " + idEnergyCommunityProcess
        
        # logging.info("Update de la tabla de procesos: " + sqlUpdateConsumos)
        logging.info("Insert de la tabla de procesos: " + sqlUpdateConsumos)
        agenteEjecucionMySql.ejecutar(sqlUpdateConsumos)
        agenteEjecucionMySql.commitTransaction()
        
        sqlConsulta = "SELECT id_energy_community_process FROM leading_db.energy_community_process WHERE (id_energy_community = "+str(idEnergyCommunity)+" AND event_id = 40 AND start = '" + sFechaActual + "');"
        auxiliar = agenteEjecucionMySql.ejecutar(sqlConsulta)
        idEnergyCommunityProcess = str(auxiliar[0][0])
        
        

        stringToResult = []
        stringToResult.append(idEnergyCommunityProcess)
        stringToResult.append(idEnergyCommunity)
        stringToResult.append(str(anyo) + "-01-01" + " 00:00:00")
        stringToResult.append(str(anyo) + "-12-31" + " 23:59:59")
        stringToResult.append(sFechaActual)

        # Mensaje de log al inicio del método
        logging.info(sFechaActual + " --> ComunidadesEnergeticasServicio.obtenerParametrosEjecucionSimulacion: Fin de la ejecución del método")

        # Damos por finalizada la ejecución
        return stringToResult
    except Exception as e:
        logging.exception(e)

def obtenerParametrosEjecucionSimulacion2(agenteEjecucionMySql,anyo,idEnergyCommunity):
    """
        author: fdgregorio
        
        traductor: jnaveiro
    
        Definición: Método encargado de obtener los parametros de ejecución de base de datos
        
        Variables de entrada: agenteEjecucionMySql,anyo(int)
        
        Variables de salida: stringToResult(list)
        
        Objetos relevantes: Ninguno
        
        Librerías que se utilizan:
        
            1. numpy
            2. datetime
            3. logging
    """
    try:
        # Mensaje de log al inicio del método
        logging.info(datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S') + " --> ComunidadesEnergeticasServicio.obtenerParametrosEjecucionSimulacion: Inicio   de la ejecución del método")

        # Por cada usuario cargamos su lista de consumos
        # private double consumos [][] = new double [3][7]
        # Consultamos si nos toca ejecutar
        sqlCommunityProcess = "SELECT * FROM leading_db.energy_community_process a WHERE ((a.event_id = 35 AND a.result=1000) OR (a.event_id = 40 AND a.result =1000) OR (a.event_id = 40 AND a.result =1001)) AND a.id_energy_community = "+ idEnergyCommunity +" AND a.start = (SELECT max(b.start) FROM leading_db.energy_community_process b WHERE a.id_energy_community = b.id_energy_community);"

        rs_communityProcess = agenteEjecucionMySql.ejecutar(sqlCommunityProcess)

        # Nos quedamos con el seleccionado por el random
        idEnergyCommunityProcess = str(rs_communityProcess[0][0])
        logging.info("idEnergyCommunity: " + str(idEnergyCommunity))

        # # Fecha comienzo y de fin
        logging.info("Anyo de la simulación: " + str(anyo))

        # DateFormat sdfMYSQL = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        sFechaActual = datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S')

        stringToResult = []
        stringToResult.append(idEnergyCommunityProcess)
        stringToResult.append(idEnergyCommunity)
        stringToResult.append(str(anyo) + "-01-01" + " 00:00:00")
        stringToResult.append(str(anyo) + "-12-31" + " 23:59:59")
        stringToResult.append(sFechaActual)

        # Mensaje de log al inicio del método
        logging.info(sFechaActual + " --> ComunidadesEnergeticasServicio.obtenerParametrosEjecucionSimulacion: Fin de la ejecución del método")

        # Damos por finalizada la ejecución
        return stringToResult
    except Exception as e:
        logging.exception(e)

def actualizarProceso(agenteEjecucionMySql, idEnergyCommunityProcess, start, event_id, codigo):
    try:
        # Mensaje de log al inicio del método
        logging.info(datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S')+" --> ComunidadesEnergeticasServicio.establecerFinEjecucionSimulacion: Inicio de la ejecución del método")

        # Establecemos la fecha de inicio de la ejecución en la correspondiente entrada
        # DateFormat sdfMYSQL = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        # String sFechaActual = sdfMYSQL.format(new Date())
        sFechaActual = datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S')

        sqlUpdateProcesoFinExito = "UPDATE leading_db.energy_community_process SET stop = '" + sFechaActual + "' WHERE (id_energy_community_process = " + idEnergyCommunityProcess + " AND event_id =" + str(event_id) +" AND start='"+ start + "');"

        logging.info ("sentenciaInsertFinExito: "+sqlUpdateProcesoFinExito)
        
        # LANZAMOS LA ACTUALIZACION DEL REGISTRO 
        agenteEjecucionMySql.ejecutar(sqlUpdateProcesoFinExito)
        agenteEjecucionMySql.commitTransaction()

        establecerFinEjecucionSimulacion(agenteEjecucionMySql, idEnergyCommunityProcess, event_id, codigo)

        # Mensaje de log al inicio del método
        logging.info (datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S')+" --> ComunidadesEnergeticasServicio.actualizarProceso: Fin de la ejecución del método")

    except Exception as e:
        logging.exception(e)


def  establecerFinEjecucionSimulacion (agenteEjecucionMySql, idEnergyCommunityProcess, event_id, codigo):
    """
        author: fdgregorio
        
        traductor: jnaveiro
    
        Definición: Método encargado de establecer como finalizada la ejecución de la simulación en base de datos
        
        Variables de entrada: agenteEjecucionMySql, idEnergyCommunityProcess(str), codigo(str)
        
        Variables de salida: Ninguna
        
        Objetos relevantes: Ninguno
        
        Librerías que se utilizan:
        
            1. datetime
            2. logging
    """
    try:
        # Mensaje de log al inicio del método
        logging.info(datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S')+" --> ComunidadesEnergeticasServicio.establecerFinEjecucionSimulacion: Inicio de la ejecución del método")

        # Establecemos la fecha de inicio de la ejecución en la correspondiente entrada
        # DateFormat sdfMYSQL = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        # String sFechaActual = sdfMYSQL.format(new Date())
        # sFechaActual = datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S')

        sqlUpdateProcesoFinExito = "UPDATE leading_db.energy_community_process SET result = " + codigo + " WHERE (id_energy_community_process = " + idEnergyCommunityProcess + " AND event_id =" + str(event_id) + ");"

        logging.info ("sentenciaInsertFinExito: "+sqlUpdateProcesoFinExito)

        # LANZAMOS LA ACTUALIZACION DEL REGISTRO 
        agenteEjecucionMySql.ejecutar(sqlUpdateProcesoFinExito)
        agenteEjecucionMySql.commitTransaction()

        # Mensaje de log al inicio del método
        logging.info (datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S')+" --> ComunidadesEnergeticasServicio.establecerFinEjecucionSimulacion: Fin de la ejecución del método")
    
    except Exception as e:
        logging.exception(e)
