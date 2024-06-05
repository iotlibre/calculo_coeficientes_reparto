import datetime
from DTOs.constantes import bisiestoA
import logging
from DTOs.constantes import ANYOELEGIDO as Anyo

# ''' Niveles de logging
# Para obtener _TODO_ el detalle: level=logging.info
# Para comprobar los posibles problemas level=logging.warning
# Para comprobar el funcionamiento: level=logging.DEBUG
# '''

#
# Objeto DTO (data object transfer).
# 
# @author fgregorio
#
# @traductor jnaveiro
#

if bisiestoA(Anyo):
    NUM_DIAS = 366
else:
    NUM_DIAS = 365

NUM_HORAS = 24

class ComunidadEnergeticaDTO:
    """
    author: fgregorio

    traductor: jnaveiro

    Definición: Esta clase pertenece al grupo DTO(data object transfer). Su misión es transmitir la información de los usuarios de las comunidades energéticas.
    
    Propiedades:

        1.idComunidadEnergetica
        2.dsComunidadEnergetica
        3.usuariosComunidad
        4.generadoresComunidad
        5.cuotaParticipacion_min
        6.cuotaParticipacion_max
        7.porcentajeDedicadoPobrezaEnergetica
    
    Métodos:

         1. ComunidadEnergeticaDTO.obtenerCoeficientesReparto_normalizadoByDemandaEnergia
         2. ComunidadEnergeticaDTO.obtenerCoeficientesReparto_cumplirCondiciones_cuotaMinima
         3. ComunidadEnergeticaDTO.obtenerCoeficientesReparto_cumplirCondiciones_cuotaMaxima
         4. ComunidadEnergeticaDTO.obtenerPrevisionEnergiaAsignadaByCoeficientesReparto
         5. ComunidadEnergeticaDTO.obtenerPrevisionExcedenteAsignadoByCoeficientesReparto
         6. ComunidadEnergeticaDTO.obtenerCuotaUtilizacionUsuariosComunidadEnergetica
         7. ComunidadEnergeticaDTO.imprimirCoeficientesRepartoClientes
         8. ComunidadEnergeticaDTO.imprimirPrevisionEnergiaAsignadaByCoeficientesReparto
         9. ComunidadEnergeticaDTO.imprimirPrevisionExcedenteAsignadoByCoeficientesReparto
        10. ComunidadEnergeticaDTO.imprimirCuotaUtilizacionComunidadEnergetica
        11. ComunidadEnergeticaDTO.getIdComunidadEnergetica
        12. ComunidadEnergeticaDTO.setIdComunidadEnergetica
        13. ComunidadEnergeticaDTO.getDsComunidadEnergetica
        14. ComunidadEnergeticaDTO.setDsComunidadEnergetica
        15. ComunidadEnergeticaDTO.getUsuariosComunidad
        16. ComunidadEnergeticaDTO.setUsuariosComunidad
        17. ComunidadEnergeticaDTO.getGeneradoresComunidad
        18. ComunidadEnergeticaDTO.setGeneradoresComunidad
        19. ComunidadEnergeticaDTO.getCuotaParticipacion_min
        20. ComunidadEnergeticaDTO.setCuotaParticipacion_min
        21. ComunidadEnergeticaDTO.getCuotaParticipacion_max
        22. ComunidadEnergeticaDTO.setCuotaParticipacion_max
        23. ComunidadEnergeticaDTO.getPorcentajeDedicadoPobrezaEnergetica
        24. ComunidadEnergeticaDTO.setPorcentajeDedicadoPobrezaEnergetica
        """
    # 
    # Atributos correspondientes a la comunidad energética 
    # 
    def __init__(self,idComunidadEnergetica = "",dsComunidadEnergetica = ""):

        self.idComunidadEnergetica = idComunidadEnergetica
        self.dsComunidadEnergetica = dsComunidadEnergetica
        self.usuariosComunidad = []
        self.generadoresComunidad = []
        self.cuotaParticipacion_min = 0 # No puede ser > 100/numClientes
        self.cuotaParticipacion_max = 100 # No puede ser < 100/numClientes
        self.porcentajeDedicadoPobrezaEnergetica = 0

    def obtenerCoeficientesReparto_normalizadoByDemandaEnergia (self):
        """
        Definición: Método encargado de obtener los coeficientes de reparto a partir de los consumos de los diversos clientes de la comunidad energética. Se calcula obteniendo para cada hora de cada día el consumo total para después dividir el consumo de cada usuario para esa hora de ese día entre el consumo total.
        
        Variables de entrada: Ninguna

        Variables de salida: Ninguna

        Propiedades modificadas:

            1. usuariosComunidad (Coeficientes de reparto)
        """
        
        #     Recorremos cada día y hora, obteniendo el coeficiente de reparto para cada cliente
                
        for it_dia in range(NUM_DIAS):
            for it_hora in range(NUM_HORAS):

                #     Declaramos la variable que contendrá el total del consumo de la comunidad para esa hora
                consumoTotalComunidad_diaHora = 0.0
                
                #     Realizamos una iteración por los clientes para obtener el total del consumo previsto para la comunidad
                usuariosComunidad = self.getUsuariosComunidad()
                for it_cliente in range(len(usuariosComunidad)):
                    usuarioAux =usuariosComunidad[it_cliente]
                    consumosAux = usuarioAux.getConsumos()
                    if(consumosAux[it_dia][it_hora] != None):
                        consumoTotalComunidad_diaHora = consumoTotalComunidad_diaHora + usuariosComunidad[it_cliente].getConsumos()[it_dia][it_hora].getValorDatoConsumoHorario()
                
                #     Paso 1: Recorremos de nuevo para asignar el coeficiente de reparto para cada cliente
                for it_cliente in range(len(usuariosComunidad)):
                    if(usuariosComunidad[it_cliente].getConsumos()[it_dia][it_hora] != None):
                        coeficiente_base = (usuariosComunidad[it_cliente].getConsumos()[it_dia][it_hora].getValorDatoConsumoHorario() / consumoTotalComunidad_diaHora)
                        pobreza_energ = (100 - self.getPorcentajeDedicadoPobrezaEnergetica())
                        usuariosComunidad[it_cliente].getCoeficientesReparto()[it_dia][it_hora] = coeficiente_base * pobreza_energ

    def obtenerCoeficientesReparto_cumplirCondiciones_cuotaMinima (self, iterar_metodo: bool, numInteracion: int):
        """
        Definición: Método encargado de hacer cumplir el mínimo establecido en los coeficientes de reparto. Lo que se hace es imponer el valor de cuota mínima a aquellos coeficientes cuyo valor sea menor. Es un proceso iterativo en el que se reducen las ganancias de todos los usuarios que pasen del mínimo para ayudar a los que no llegan a ese mínimo. Para ello, además de imponer el valor de cuota mínima, calcula qué porcentaje se necesita retirar de los demás usuarios y reparte lo que hay que retirar entre todos ellos. Tiene que realizar esta iteración varias veces hasta que ningún usuario esté por debajo del mínimo. La cuota mínima debe de ser menor que 100/(Número de usuarios de la comunidad), sino no converge.
        
        Variables de entrada: iterar_metodo: bool, numInteracion: int

        Variables de salida: Ninguna

        Propiedades modificadas:

            1. usuariosComunidad (Coeficientes de reparto)
        """
        
        #     Recorremos cada día y hora, obteniendo el coeficiente de reparto para cada cliente
                
        for it_dia in range(NUM_DIAS):
            for it_hora in range(NUM_HORAS):

                #     PASO 1: Chequemos quien y cuanto cumple o no cumple. 
                diferenciaCoeficienteTotal_noCumple_minimo = 0.0
                diferenciaCoeficienteTotal_siCumple_minimo = 0.0
                #     Recorremos obteniendo los datos preliminales
                for it_cliente in range(len(self.usuariosComunidad)):
                    cliente_it = self.getUsuariosComunidad()[it_cliente]
                    coeficienteReparto= cliente_it.getCoeficientesReparto()[it_dia][it_hora]
                    minimoRepartoCoeficiente = self.getCuotaParticipacion_min()
                    if(coeficienteReparto<=minimoRepartoCoeficiente):
                        diferenciaCoeficienteTotal_noCumple_minimo = diferenciaCoeficienteTotal_noCumple_minimo + (minimoRepartoCoeficiente - coeficienteReparto)
                        cliente_it.getCoeficientesReparto()[it_dia][it_hora] = minimoRepartoCoeficiente
                    else:
                        diferenciaCoeficienteTotal_siCumple_minimo = diferenciaCoeficienteTotal_siCumple_minimo + coeficienteReparto

                #     fin for

                #     Condición método recursivo
                if(diferenciaCoeficienteTotal_noCumple_minimo != 0 ): #  No hay ninguna diferencia y hemos acabado
                    
                    # Paso 2: Recorremos únicamente los que son mayores que el mímimo restandole lo que falta de manera podenderada
                    for it_cliente in range(len(self.usuariosComunidad)):
                        
                        cliente_it = self.getUsuariosComunidad()[it_cliente]
                        coeficienteReparto= cliente_it.getCoeficientesReparto()[it_dia][it_hora]
                        minimoRepartoCoeficiente = self.getCuotaParticipacion_min()
                        
                        if(coeficienteReparto>minimoRepartoCoeficiente): #  Es decir, que tiene margen
                            cliente_it.getCoeficientesReparto()[it_dia][it_hora] = cliente_it.getCoeficientesReparto()[it_dia][it_hora] - (cliente_it.getCoeficientesReparto()[it_dia][it_hora]/diferenciaCoeficienteTotal_siCumple_minimo) * diferenciaCoeficienteTotal_noCumple_minimo
                        
                    
                    
                    # Como no sabemos si con el cambio hemos estropeado alguno que antes SI cumplia, iteramos
                    if(iterar_metodo & numInteracion <20):
                        
                        #     Imprimimos tras cada iteración el resultado particial que llevamos
                        #     self.imprimirCoeficientesRepartoClientes()
                        
                        #    print("Ejecutamos una nueva iteración del metodo participacionMinima")
                        self.obtenerCoeficientesReparto_cumplirCondiciones_cuotaMinima(iterar_metodo, numInteracion+1)
                    
                    
                #     fin if
            #    for hora
        #     for dia

    def obtenerCoeficientesReparto_cumplirCondiciones_cuotaMaxima (self, iterar_metodo: bool, numInteracion: int):
        """
        Definición: Método encargado de hacer cumplir el máximo establecido en los coeficientes de reparto.. Lo que hace es imponer que el valor de cuota Máxima a aquellos que superen este valor. Es un proceso iterativo en el que se reduce las ganancias de  los usuarios que pasen del máximo para para repartirlo. Para ello, además de imponer la cuota máxima, calcula qué porcentaje se añade a los demás usuarios y reparte lo que hay entre todos ellos. Tiene que realizar esta iteración varias veces hasta que ningún usuario esté por encima del máximo. La cuota máxima debe de ser mayor que 100/(Número de usuarios de la comunidad), sino no converge
        
        Variables de entrada: iterar_metodo: bool, numInteracion: int

        Variables de salida: Ninguna

        Propiedades modificadas:

            1. usuariosComunidad (Coeficientes de reparto)
        """
                
        #     Recorremos cada día y hora, obteniendo el coeficiente de reparto para cada cliente
            
        #     Obtenemos el máximo del coeficiente de reparto para la comunidad
        maximoRepartoCoeficiente = float(self.getCuotaParticipacion_max())
        
        for it_dia in range(NUM_DIAS):
            for it_hora in range(NUM_HORAS):

                #     PASO 1: Chequemos quien y cuanto cumple o no cumple. 
                diferenciaCoeficienteTotal_noCumple_maximo = 0.0
                diferenciaCoeficienteTotal_siCumple_maximo = 0.0
                #     Recorremos obteniendo los datos preliminales
                for it_cliente in range(len(self.usuariosComunidad)):
                    cliente_it = self.getUsuariosComunidad()[it_cliente]
                    coeficienteReparto = cliente_it.getCoeficientesReparto()[it_dia][it_hora]
                    if(coeficienteReparto>=maximoRepartoCoeficiente):
                        diferenciaCoeficienteTotal_noCumple_maximo = diferenciaCoeficienteTotal_noCumple_maximo + (maximoRepartoCoeficiente - coeficienteReparto)
                        cliente_it.getCoeficientesReparto()[it_dia][it_hora] = maximoRepartoCoeficiente
                    else:
                        diferenciaCoeficienteTotal_siCumple_maximo = diferenciaCoeficienteTotal_siCumple_maximo + coeficienteReparto
                    
                 #     fin for
                
                #     Condición método recursivo
                if(diferenciaCoeficienteTotal_noCumple_maximo!=0 ):
                    #     No hay ninguna diferencia y hemos acabado
                    
                    #     Paso 2: Recorremos únicamente los que son mayores que el mímimo restandole lo que falta de manera podenderada
                    for it_cliente in range(len(self.usuariosComunidad)):
                        
                        cliente_it = self.getUsuariosComunidad()[it_cliente]
                        coeficienteReparto= cliente_it.getCoeficientesReparto()[it_dia][it_hora]

                        if(coeficienteReparto<maximoRepartoCoeficiente): #     Es decir, que tiene margen
                            cliente_it.getCoeficientesReparto()[it_dia][it_hora] = cliente_it.getCoeficientesReparto()[it_dia][it_hora] + (cliente_it.getCoeficientesReparto()[it_dia][it_hora]/diferenciaCoeficienteTotal_siCumple_maximo) * diferenciaCoeficienteTotal_noCumple_maximo
                        
                    
                    
                    #     Como no sabemos si con el cambio hemos estropeado alguno que antes SI cumplia, iteramos
                    if(iterar_metodo & numInteracion <20 ):
                        
                        #     Imprimimos tras cada iteración el resultado particial que llevamos
                        #    self.imprimirCoeficientesRepartoClientes()
                        
                        #     print("Ejecutamos una nueva iteración del metodo participacionMaxima")
                        self.obtenerCoeficientesReparto_cumplirCondiciones_cuotaMaxima(iterar_metodo, numInteracion+1)
                    
                    
                #     fin if
             #    for hora
         #     for dia

    def obtenerPrevisionEnergiaAsignadaByCoeficientesReparto (self):
        """
        Definición: Método encargado de obtener la previsión de energía para cada usuario en función de su coeficiente de reparto. A partir de la generación prevista, teniendo los coeficientes de reparto ya calculados, multiplica la generación total por el coeficiente correspondiente.
        
        Variables de entrada: Ninguna

        Variables de salida: Ninguna

        Propiedades modificadas:

            1. usuariosComunidad (Energía de reparto)
        """
        #     Recorremos cada día y hora, obteniendo el coeficiente de reparto para cada cliente
                
        #     Realizamos una iteración por los clientes para obtener el total del consumo previsto para la comunidad
        usuariosComunidad = self.getUsuariosComunidad()
        for it_cliente in range(len(usuariosComunidad)):
            
            #     Creamos la matriz a rellenar con los valores del reparto de energía
            energiaRepartirCliente = [[None for i in range(NUM_HORAS)] for j in range(NUM_DIAS)]

            for it_dia in range(NUM_DIAS):
                for it_hora in range(NUM_HORAS):
    
                    #     Obtenemos la previsión de energía disponible
                    totalGeneracionEnergiaPrevista_diahora = 0.0
                    listGeneradores = self.getGeneradoresComunidad()
                    for it_generadores in range(len(listGeneradores)):
                        totalGeneracionEnergiaPrevista_diahora = totalGeneracionEnergiaPrevista_diahora + listGeneradores[it_generadores].getGeneracion()[it_dia][it_hora]
                    
                    
                    coeficienteRepartoCliente = usuariosComunidad[it_cliente].getCoeficientesReparto()[it_dia][it_hora]
                    energiaDisponibleClienteDiaHora = (coeficienteRepartoCliente/100) * totalGeneracionEnergiaPrevista_diahora
                    energiaRepartirCliente [it_dia] [it_hora] = energiaDisponibleClienteDiaHora
                    
                 #    for hora
             #     for dia
        
            usuariosComunidad[it_cliente].setEnergiaReparto(energiaRepartirCliente)
        
        #     for cliente

    def obtenerPrevisionExcedenteAsignadoByCoeficientesReparto (self):
        """
        Definición: Método encargado de obtener los excedentes del cliente en base a su consumo. Teniendo la energía que le corresponde a cada usuario de la generada, calcula si el usuario tiene excedentes.
        
        Variables de entrada: Ninguna

        Variables de salida: Ninguna

        Propiedades modificadas:

            1. usuariosComunidad (Energía de reparto excedentes)
        """
                
        #     Recorremos cada día y hora, obteniendo el coeficiente de reparto para cada cliente
                
        #     Realizamos una iteración por los clientes para obtener el total del consumo previsto para la comunidad
        usuariosComunidad = self.getUsuariosComunidad()
        for it_cliente in range(len(usuariosComunidad)):
            
            #     Creamos la matriz a rellenar con los valores del reparto de energía
            excedenteEnergia = [[None for i in range(NUM_HORAS)] for j in range(NUM_DIAS)]
            
            for it_dia in range(NUM_DIAS):
                for it_hora in range(NUM_HORAS):
                    consumoClienteDiaHora = 0.0
                    if(usuariosComunidad[it_cliente].getConsumos()[it_dia][it_hora] != None):
                        consumoClienteDiaHora = usuariosComunidad[it_cliente].getConsumos()[it_dia][it_hora].getValorDatoConsumoHorario()
                    energiaAsignadaClienteDiaHora =  usuariosComunidad[it_cliente].getEnergiaReparto()[it_dia][it_hora]
                    excedenteEnergiaClienteDiaHora = energiaAsignadaClienteDiaHora - consumoClienteDiaHora
                    if(excedenteEnergiaClienteDiaHora<0):
                        excedenteEnergiaClienteDiaHora = 0
                    
                    excedenteEnergia [it_dia][it_hora] = excedenteEnergiaClienteDiaHora
                    
                #    for hora
            #     for dia
        
            usuariosComunidad[it_cliente].setEnergiaReparto_excedentes(excedenteEnergia)
        
        #     for cliente

    def obtenerCuotaUtilizacionUsuariosComunidadEnergetica (self):
        """
        Definición: Método encargado de calcular la cuota de participación base a los coeficientes de reparto del cliente. Calcula el promedio de participación de cada usuario.
        
        Variables de entrada: Ninguna

        Variables de salida: Ninguna

        Propiedades modificadas:

            1. usuariosComunidad (Cuota de participación a partir de los coeficientes de reparto)
        """
        #     Recorremos cada día y hora, obteniendo el coeficiente de reparto para cada cliente

        #     Realizamos una iteración por los clientes para obtener el total del consumo previsto para la comunidad
        usuariosComunidad = self.getUsuariosComunidad()
        for it_cliente in range(len(usuariosComunidad)):
            
            #     Creamos la matriz a rellenar con los valores del reparto de energía
            contadorCuotasParticipacion = 0
            cuotaParticipacionCalculadaCE = 0.0
            
            for it_dia in range(NUM_DIAS):
                for it_hora in range(NUM_HORAS):
                    if (usuariosComunidad[it_cliente].getConsumos()[it_dia][it_hora] != None):
                        contadorCuotasParticipacion += 1
                        cuotaParticipacionCalculadaCE = cuotaParticipacionCalculadaCE + usuariosComunidad[it_cliente].getCoeficientesReparto()[it_dia][it_hora]
                 #    for hora
             #     for dia
        
            usuariosComunidad[it_cliente].setCuotaParticipacion_calculadaCR(cuotaParticipacionCalculadaCE/contadorCuotasParticipacion)
        
        #     for cliente

    def imprimirCoeficientesRepartoClientes (self):
        """
        Definición: Método utilizado para imprimir por archivo log la estructura de datos correspondiente al coeficiente de reparto.
        
        Variables de entrada: Ninguna

        Variables de salida: Ninguna

        Propiedades modificadas: Ninguna
        """
        usuariosComunidad = self.getUsuariosComunidad()
        for it_cliente in range(len(usuariosComunidad)):
            logging.info("\n")
            logging.info(" Cliente: " , usuariosComunidad[it_cliente].getIdUsuario())
            logging.info("\n")
            for it_dia in range(NUM_DIAS):
                for it_hora in range(NUM_HORAS):
                    logging.info("Coeficiente reparto [" + str(it_dia) + "] [" + str(it_hora) + "]:" + str(usuariosComunidad[it_cliente].getCoeficientesReparto()[it_dia][it_hora]))

    def imprimirPrevisionEnergiaAsignadaByCoeficientesReparto (self):
        """
        Definición: Método utilizado para imprimir por log el reparto de energía de los clientes.
        
        Variables de entrada: Ninguna

        Variables de salida: Ninguna

        Propiedades modificadas: Ninguna
        """
         
        usuariosComunidad = self.getUsuariosComunidad()
        for it_cliente in range(len(usuariosComunidad)):
            logging.info("\n")
            logging.info(" Cliente: " , usuariosComunidad[it_cliente].getIdUsuario())
            logging.info("\n")
            for it_dia in range(NUM_DIAS):
                for it_hora in range(NUM_HORAS):
                    logging.info("Energia asignada cliente [hora][dia]: [" + str(it_dia) + "] [" + str(it_hora) + "]:" + str(usuariosComunidad[it_cliente].getEnergiaReparto()[it_dia][it_hora]))

    def imprimirPrevisionExcedenteAsignadoByCoeficientesReparto (self):
        """
        Definición: Método empleado para imprimir por log los excedentes de energía de los clientes respecto su demanda energética.
        
        Variables de entrada: Ninguna

        Variables de salida: Ninguna

        Propiedades modificadas: Ninguna
        """
        usuariosComunidad = self.getUsuariosComunidad()
        for it_cliente in range(len(usuariosComunidad)):
            logging.info("\n")
            logging.info(" Cliente: " + str(usuariosComunidad[it_cliente].getIdUsuario()))
            logging.info("\n")
            for it_dia in range(NUM_DIAS):
                for it_hora in range(NUM_HORAS):
                    logging.info("Energia excedente cliente [hora][dia]: [" + str(it_dia) + "] [" + str(it_hora) + "]:" + str(usuariosComunidad[it_cliente].getEnergiaReparto_excedentes()[it_dia][it_hora]))

    def imprimirCuotaUtilizacionComunidadEnergetica (self):
        """
        Definición: Método utilizado para imprimir por log el coeficiente de participación de los clientes en base a su coeficiente de reparto dinámico.

        Variables de entrada: Ninguna

        Variables de salida: Ninguna

        Propiedades modificadas: Ninguna
        """
        usuariosComunidad = self.getUsuariosComunidad()
        for it_cliente in range(len(usuariosComunidad)):
            logging.info("Cuota de utilización calculada (en base a los CR): Cliente " + str(usuariosComunidad[it_cliente].getIdUsuario()) + " : " + str(usuariosComunidad[it_cliente].getCuotaParticipacion_calculadaCR()))

    def getIdComunidadEnergetica(self):
        return self.idComunidadEnergetica

    def setIdComunidadEnergetica(self, idComunidadEnergetica: str):
        self.idComunidadEnergetica = idComunidadEnergetica

    def getDsComunidadEnergetica(self):
        return self.dsComunidadEnergetica

    def setDsComunidadEnergetica(self, dsComunidadEnergetica: str):
        self.dsComunidadEnergetica = dsComunidadEnergetica

    def getUsuariosComunidad(self):
        return self.usuariosComunidad

    def setUsuariosComunidad(self, usuariosComunidad):
        self.usuariosComunidad = usuariosComunidad

    def getGeneradoresComunidad(self):
        return self.generadoresComunidad

    def setGeneradoresComunidad(self, generadoresComunidad):
        self.generadoresComunidad = generadoresComunidad

    def getCuotaParticipacion_min(self):
        return self.cuotaParticipacion_min

    def setCuotaParticipacion_min(self,cuotaParticipacion_min: float):
        self.cuotaParticipacion_min = cuotaParticipacion_min

    def getCuotaParticipacion_max(self):
        return self.cuotaParticipacion_max

    def setCuotaParticipacion_max(self,cuotaParticipacion_max: float):
        self.cuotaParticipacion_max = cuotaParticipacion_max

    def getPorcentajeDedicadoPobrezaEnergetica(self):
        return self.porcentajeDedicadoPobrezaEnergetica
    
    def  setPorcentajeDedicadoPobrezaEnergetica(self, porcentajeDedicadoPobrezaEnergetica: float):
        self.porcentajeDedicadoPobrezaEnergetica = porcentajeDedicadoPobrezaEnergetica
