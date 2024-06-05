from datetime import datetime as tiem 

from agents import MySqlAgent_AEI2022_LEADING

import ComunidadesEnergeticasServicio as ComunidadesEnergeticasServicio
from DTOs.constantes import ANYOELEGIDO as Anyo

import sys
import logging
from logging.handlers import RotatingFileHandler

# 
# Tarea usada para la ejecución de la simulación de los coeficientes de reparto dinámicos.
# 
# @author fgregorio
# 
# @traductor jnaveiro
# 

''' Niveles de logging
Para obtener _TODO_ el detalle: level=logging.info
Para comprobar los posibles problemas level=logging.warning
Para comprobar el funcionamiento: level=logging.DEBUG
'''

logging.basicConfig(
        level=logging.DEBUG,
        handlers=[RotatingFileHandler('./AEI_LEADING_MAIN_python_Output.log', maxBytes=1000000, backupCount=4)],
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p')

def simulador():
    """
    Definición: Esta función es la principal de ejecución del proceso de cálculo de coeficientes de reparto. Realiza las consultas a la base de datos y, una vez se tiene la información, calcula los coeficientes de reparto para actualizarlos en la bas de datos.
    
    Variables de entrada: No tiene
    
    Variables de salida: No tiene
    
    Objetos relevantes:
    
        1. agenteEjecucionMySql
        2. comunidadEnergetica
    
    Librerías que se llaman:
    
        1. Simulador.ComunidadesEnergeticasServicio
        2. Simulador.agents.MySqlAgent_AEI2022_LEADING
        3. datetime
        4. logging
        5. sys
        """
    #Variable para almacenar el id del proceso a ejecutar
    idEnergyCommunity = ""
    
    try:    

        #Indicamos que el programa se ha iniciado
        sdfSegundos = tiem.now()
        print(sdfSegundos.__format__('%d/%m/%Y %H:%M:%S') +": Lanzada de forma correcta la ejecución del Software de Simulación CE ")
        print(sdfSegundos.__format__('%d/%m/%Y %H:%M:%S') +": Log de la ejecución volcando en el fichero 'AEI_LEADING_MAIN_python_Output.log'")
        
        #Mostramos en el log el inicio de la realización de la tarea
        sdfSegundos = tiem.now()
        logging.info(" - Task: CaracterizacionComunidadesEnergeticasTask: -> Start exec. (" + sdfSegundos.__format__('%d/%m/%Y %H:%M:%S') + ") ")

        #Paso 0: Parametros generales de la simulación
        #Obtenemos el agente de base de datos que utilizaremos durante toda la ejecución
        agenteEjecucionMySql = MySqlAgent_AEI2022_LEADING()
        
        print("Hay una conexión válida: ", agenteEjecucionMySql.isValidConection())
        
        parametrosSimulacion = []
                
        #Si no existen simulaciones pendientes, entonces acabamos.
        try:    
            parametrosSimulacion = ComunidadesEnergeticasServicio.obtenerParametrosEjecucionSimulacion(agenteEjecucionMySql,Anyo)
        except:
            logging.exception("EVENT_ID=40 NO HAY COM. ENERG. PENDIENTES DE SIMULAR")
        
        if (len(parametrosSimulacion)>3):
            idEnergyCommunityProcess = parametrosSimulacion[0]#""#"0" 
            idEnergyCommunity = parametrosSimulacion[1] #"2" 
            simulacion_fcDesde = parametrosSimulacion[2] #"2021-01-01 00:00:00"
            simulacion_fcHasta = parametrosSimulacion[3] #"2021-12-31 23:59:59"
        else:
            idEnergyCommunityProcess = ""#""#"0" 
            idEnergyCommunity = "" #"2" 
            simulacion_fcDesde = "" #"2021-01-01 00:00:00"
            simulacion_fcHasta = "" #"2021-12-31 23:59:59"
                
        #Realizamos la consulta para recuperar los parámetros de la ejecución
        if (len(idEnergyCommunityProcess) != 0 and len(idEnergyCommunity) != 0 and len(simulacion_fcDesde) != 0 and len(simulacion_fcHasta) != 0):
            #Paso 1: Recupeamos e inicializamos las instancias con los datos recuperados de base de datos
            logging.info("\n")
            logging.info(tiem.now().__format__('%d/%m/%Y %H:%M:%S')+" -> Paso 1: CaracterizacionComunidadesEnergeticasTask: Obtenemos de base de datos los datos iniciales de la comunidad (Clientes y generadores)")
            logging.info("\n")
            comunidadEnergetica = ComunidadesEnergeticasServicio.obtenerDatosComunidadEnergeticaDesdeBBDD(agenteEjecucionMySql, idEnergyCommunity, simulacion_fcDesde, simulacion_fcHasta)
            
            #Paso 2: Ejecutamos e imprimimos el cálculo del coeficiente de reparto
            logging.info("\n")
            logging.info(tiem.now().__format__('%d/%m/%Y %H:%M:%S')+" -> Paso 2: CaracterizacionComunidadesEnergeticasTask: Ejecutamos el algoritmo de cálculo de coeficientes de reparto calculados en base a la demanda energética")
            logging.info("\n")
            comunidadEnergetica.obtenerCoeficientesReparto_normalizadoByDemandaEnergia()
            #comunidadEnergetica.imprimirCoeficientesRepartoClientes()
            
            #Paso 3: Ejecutamos los cálculos para el cumplimiento de las condiciones establecidas
            logging.info("\n")
            logging.info(tiem.now().__format__('%d/%m/%Y %H:%M:%S')+" -> Paso 3: CaracterizacionComunidadesEnergeticasTask: Ejecutamos el algoritmo para el cumplimiento de las condiciones (part. max. y min)")
            logging.info("\n")
            comunidadEnergetica.obtenerCoeficientesReparto_cumplirCondiciones_cuotaMinima(True, 1)
            comunidadEnergetica.obtenerCoeficientesReparto_cumplirCondiciones_cuotaMaxima(True, 1)
            #comunidadEnergetica.imprimirCoeficientesRepartoClientes()

            #Paso 4: Estimamos el reparto de la energía y lo imprimimos
            logging.info("\n")
            logging.info(tiem.now().__format__('%d/%m/%Y %H:%M:%S')+" -> Paso 4: CaracterizacionComunidadesEnergeticasTask: Ejecutamos el algoritmo para el reparto de la energía")
            logging.info("\n")
            comunidadEnergetica.obtenerPrevisionEnergiaAsignadaByCoeficientesReparto()
            #comunidadEnergetica.imprimirPrevisionEnergiaAsignadaByCoeficientesReparto()
            
            #Paso 5: Calculamos los excedentes para cada cliente (en el caso de que los haya)
            logging.info("\n")
            logging.info(tiem.now().__format__('%d/%m/%Y %H:%M:%S')+" -> Paso 5: CaracterizacionComunidadesEnergeticasTask: Ejecutamos el algoritmo para el cálculo del excedente de energía")
            logging.info("\n")
            comunidadEnergetica.obtenerPrevisionExcedenteAsignadoByCoeficientesReparto()
            #comunidadEnergetica.imprimirPrevisionExcedenteAsignadoByCoeficientesReparto()
            
            #Paso 6: Coeficiente de participación
            logging.info("\n")
            logging.info(tiem.now().__format__('%d/%m/%Y %H:%M:%S')+" -> Paso 6: CaracterizacionComunidadesEnergeticasTask: Ejecutamos el algoritmo para el cálculo del excedente de energía")
            logging.info("\n")
            comunidadEnergetica.obtenerCuotaUtilizacionUsuariosComunidadEnergetica()
            comunidadEnergetica.imprimirCuotaUtilizacionComunidadEnergetica()
            
            #Paso 7: Almacenamiento de los datos registrados (almacenamos en la tabla correspondiente los datos obtenidos).
            logging.info("\n")
            logging.info(tiem.now().__format__('%d/%m/%Y %H:%M:%S')+" -> Paso 7: ComunidadesEnergeticasServicio.almacenarDatosCalculadosComunidadEnergetica: Almacenamos los valores obtenidos en base de datos")
            logging.info("\n")
            ComunidadesEnergeticasServicio.almacenarDatosCalculadosComunidadEnergetica(agenteEjecucionMySql, comunidadEnergetica)
        
            #Paso 8: Establecemos en base de datos como finalizado la ejecución en la correspondiente tabla 
            logging.info("\n")
            logging.info(tiem.now().__format__('%d/%m/%Y %H:%M:%S')+" -> Paso 8: ComunidadesEnergeticasServicio.almacenarDatosCalculadosComunidadEnergetica: Almacenamos los valores obtenidos en base de datos")
            logging.info("\n")
            ComunidadesEnergeticasServicio.establecerFinEjecucionSimulacion(agenteEjecucionMySql, idEnergyCommunityProcess, "1000")
                
            #fin if: Si existen todos los parametros rellenos de base de datos
            
            #Mostramos en el log el fin de la realización de la tarea
            logging.info(" - Task: CaracterizacionComunidadesEnergeticasTask: -> End exec. (" + tiem.now().__format__('%d/%m/%Y %H:%M:%S') + ") ")
            
            sdfSegundos = tiem.now()
            print(sdfSegundos.__format__('%d/%m/%Y %H:%M:%S')+": Finalizada de forma correcta la ejecución del Software de Simulación CE ")
            
            #Damos por finalizado el programa
            #FIXME: 13/06/2022. Pendiente de probarlo en ejecución en el servidor.
            agenteEjecucionMySql.cursor.close()
            sys.exit(0)
        else:
            logging.info(" - Task: CaracterizacionComunidadesEnergeticasTask: -> Datos vacios, linea 70 (" + tiem.now().__format__('%d/%m/%Y %H:%M:%S') + ") ")
            
    except Exception as err:
        
        #Establecemos que ha ocurrido un error
        #Obtenemos el agente de base de datos que utilizaremos durante toda la ejecución
        try:    
            agenteEjecucionMySql = MySqlAgent_AEI2022_LEADING()
            ComunidadesEnergeticasServicio.establecerFinEjecucionSimulacion(agenteEjecucionMySql, idEnergyCommunity, "1001")
            agenteEjecucionMySql.cursor.close()

        except Exception as err1:
            message = "Unexpected {}, {}".format(err1,type(err1))
            logging.exception(message)
            print("Unexpected {}, {}".format(err1,type(err1)))
        
        logging.exception("Excepcion ocurrida AEI_LEADING_RepartoCoeficientesTask: " + tiem.now().__format__('%d/%m/%Y %H:%M:%S'))
        logging.exception(err)
        sys.exit(1001)

if __name__=="__main__":
    simulador()
