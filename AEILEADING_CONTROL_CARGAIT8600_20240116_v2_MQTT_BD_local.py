# -*- coding: utf-8 -*-
"""

Proceso de control de carga programable IT8600:
- Recuperación del valor de potencia a consumir en función del resultado para la hora actual de la última simulación disponible
- Establecimiento de la conexión, puesta a punto para el control remoto de la carga y asignación del valor de potencia recuperado.

Created on 16/01/2024
Modified on 16/01/2024

@author: fgregorio, gcalvo
"""

# Import para leer los parametros de entrada
import sys

# Importación de las librerias referentes a la base de datos
import mysql.connector
from mysql.connector import Error 



# Importación libreria temporal
import time
from datetime import datetime

# Importación librería conexión con la carga
import socket

# Constante seguridad tensión mínima bateria
LIMIT_MIN_BAT_VOL = 49;

# Tratamiento argumentos
param_id_community = sys.argv[1]; # Para filtrar por comunidad energética
param_limit_max_power = sys.argv[2]; # W


from lecturaIni import lecturaIni

# Parametros de BBDD
DB_USER,DB_PASS,DB_HOST,DB_PORT,DB_NAME = lecturaIni("soloLectura.ini")

try:
    
    while 1==1:

        
        # Obtenemos la fecha y hora actual del sistema
        fecha_hora_now = datetime.now().strftime("%Y-%m-%d %H:00:00");
        print("Fecha_hora actual:", fecha_hora_now)


        """
        Primera parte: Recuperación del valor máximo de potencia de la comunidad energética (todo el año)
        """
        SQL_AEILEADING_MAX_CONSUMPTION_COMMUNITY_ALLUSER = """
        SELECT EC.ID_ENERGY_COMMUNITY, user_data.timestamp, SUM(user_data.consumption)
        FROM energy_community AS EC
        INNER JOIN user AS user_community ON user_community.id_energy_community = EC.id_energy_community
        INNER JOIN user_data AS user_data ON user_data.id_user = user_community.id_user
        WHERE EC.ID_ENERGY_COMMUNITY = '""" + param_id_community + """' 
        GROUP BY EC.ID_ENERGY_COMMUNITY, user_data.timestamp
        ORDER BY SUM(user_data.consumption) desc;
        """

        # Comprobación composición de consulta
        print(SQL_AEILEADING_MAX_CONSUMPTION_COMMUNITY_ALLUSER);

        # Creamos la variable donde almacenaremos el valor máximo de consumo de la comunidad para el año completo
        power_community_max_year = 1;

        # Ejecución de la consulta y obtención de la variable de potencia maxima anual
        try:
            connection = mysql.connector.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, auth_plugin='mysql_native_password')
            if connection.is_connected():
                db_Info = connection.get_server_info()
                # print("Connected to MySQL Server version ", db_Info)
                cursor = connection.cursor()
                # cursor.execute("select database();")
                cursor.execute(SQL_AEILEADING_MAX_CONSUMPTION_COMMUNITY_ALLUSER);
                
                record = cursor.fetchone()
                
                power_community_max_year = record[2];
                
                
                                
        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            if (connection.is_connected()):
                cursor.close()
                connection.close()
                # print("MySQL connection is closed")

        # Comprobamos el valor obtenido
        print ("Valor máximo del consumo de la comunidad anual es: ", power_community_max_year);

        """
        Segunda parte: Recuperación del valor de potencia de la última simulación para la hora actual
        """

        # Consulta para recuperar el consumo de la comunidad a la hora actual
        sql_aeileading_current_consumption_community = """
        SELECT EC.ID_ENERGY_COMMUNITY, user_data.timestamp, SUM(user_data.consumption)
        FROM energy_community AS EC
        INNER JOIN user AS user_community ON user_community.id_energy_community = EC.id_energy_community
        INNER JOIN user_data AS user_data ON user_data.id_user = user_community.id_user
        WHERE EC.ID_ENERGY_COMMUNITY = '""" + param_id_community + """' 
        AND user_data.timestamp = '""" + fecha_hora_now + """' 
        GROUP BY EC.ID_ENERGY_COMMUNITY, user_data.timestamp
        ORDER BY SUM(user_data.consumption);
        """

        # Comprobación composición de consulta
        print(sql_aeileading_current_consumption_community);

        # Declaramos la variable donde almacenaremos la potencia actual de la comunidad para la hora en curso
        current_community_power = 0;

        # Ejecución de la consulta y obtención de la variable de potencia para la hora actual
        try:
            connection = mysql.connector.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, auth_plugin='mysql_native_password')
            if connection.is_connected():
                db_Info = connection.get_server_info()
                # print("Connected to MySQL Server version ", db_Info)
                cursor = connection.cursor()
                # cursor.execute("select database();")
                cursor.execute(sql_aeileading_current_consumption_community);
                
                record = cursor.fetchone()
                
                current_community_power = record[2];
                                
        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            if (connection.is_connected()):
                cursor.close()
                connection.close()
                # print("MySQL connection is closed")

        # Comprobamos el valor obtenido
        print ("El valor de consumo 'crudo' para la hora en la comunidad es: ", current_community_power);

        """
        Tercera parte: Convertimos el consumo de la comunidad en base a la escala (maximo anual) y aplicamos filtro máximo de potencia
        """
        # Convertimos la comunidad en base a su escala (valor máximo anual)
        current_community_power = (current_community_power / power_community_max_year)*3;  # kW

        # Lo convertimos en entero para dar el comando a la carga, y lo pasamos a W
        current_community_power = int (current_community_power * 1000) ; # W

        # Filtramos la potencia máxima en base al límite establecido
        if (float(current_community_power) > float(param_limit_max_power)):
                print (" WARNING: La potencia se esta límitando debido a que la consigna supera el limite maximo");
                current_community_power = float(param_limit_max_power);
                # current_community_power= round(current_community_power, 2);

        print ("El valor a ejecutar en la carga es: ", current_community_power);

        """
        Cuarta parte: Mecanismo seguridad: Comprobación tensión batería antes de asignar consigna
        """
        print ("---- COMPROBACION DE TENSION A TRAVES DE SUBPROCESO ---");
        from pyModbusTCP.client import ModbusClient
        from ctypes import *
        import time;
        import sys;



        # Funcion encargada de llevar a cabo la conversion 16uint->32float
        def convert(s):
            i = int(s, 16)                   # convert from hex to a Python int
            cp = pointer(c_int(i))           # make this into a c integer
            fp = cast(cp, POINTER(c_float))  # cast the int pointer to a float pointer
            return fp.contents.value         # dereference the pointer, get the float

        # Declaramos el objeto con los parametros correspondientes a la conexion
        modbusClient = ModbusClient()
        MD_USER,MD_PASS,MD_HOST,MD_PORT,MD_UNID = lecturaIni("soloLectura.ini")
        modbusClient.host = MD_HOST
        modbusClient.port = int(MD_PORT)
        modbusClient.unit_id = int(MD_UNID)

        # managing TCP sessions with call to c.open()/c.close()
        modbusClient.open()

        # Read 2x 16 bits registers at modbus address 0 :
        firstRegister = 771;
        numberRegister = 1;
        holdRegs = modbusClient.read_holding_registers(firstRegister, numberRegister)

        resultado_tension_bateria_victron = 0;
        if holdRegs:

                printValues = "";   
                
                # Imprimos los resultados
                for i in range (0,numberRegister):
                        # printValues = str(printValues) + "["+str(i)+"]" + str(holdRegs[i]) + str(" ");
                        printValues = holdRegs[i];

                # Impresion resultados
                fechaActual = time.strftime("%d/%m/%Y %H:%M:%S");
                #print("[%s]: [%s]" % (fechaActual, printValues));
                resultado_tension_bateria_victron = float(printValues)/100;
                
        else: 
                fechaActual = time.strftime("%d/%m/%Y %H:%M:%S");
                print("[%s]: ERROR: Registros no leidos" % fechaActual);

        # Esperamos a la siguiente lectura 
        #time.sleep(60);

        # Cerramos la conexion del cliente Modbus
        modbusClient.close();
        print("La tension de la bateria obtenida de Victron es: ", resultado_tension_bateria_victron);

        # Declaramos los atributos del socket (carga electrónica)
        ip_carga = '10.50.10.101';
        pto_carga = 30000
            
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        s.connect((ip_carga, pto_carga));
        
        if(resultado_tension_bateria_victron>LIMIT_MIN_BAT_VOL):
            
            """
            Quinta parte: Establecimiento de la conexión, parametrización de la carga en modo remoto y asignación de potencia
            """
            # Al comenzar este segundo paso, imprimimos los parámetros de entrada para comprobar
            print ("Paso 4: control de la carga: Potencia para la fecha %s es: %s" % (fecha_hora_now, current_community_power))
            
            # Modo remoto
            s.sendall(b'SYSTem:REMote\n');
            
            # Establecer el modo de funcionamiento DC
            s.sendall(b'SYSTem:MODE DC\n');
            
            # Establecer el modo de operación CP
            s.sendall(b'FUNCtion POWer\n');
            
            # Establecer potencia
            cadena_pot = "POWer " + str(current_community_power)
            cadena_pot_byte = cadena_pot.encode()
            s.sendall(cadena_pot_byte)
            s.sendall(b'\n')
            
            # Encender la carga después de mandarle una consigna
            s.sendall(b'INPut 1\n');
            
            # Modo local
            s.sendall(b'SYSTem:LOCal\n');
                
            print ("AEI_LEADING - Arrancanda carga con valor de potencia: ", current_community_power);

        else: # Si la tensión de la bateria esta por debajo de 48V paramos la carga
            print("WARNING: La tensión de la bateria es menor a la del límite ", LIMIT_MIN_BAT_VOL, ". Se detiene la carga.");
                
            # Modo remoto
            s.sendall(b'SYSTem:REMote\n');
            
            # Encender la carga después de mandarle una consigna
            s.sendall(b'INPut 0\n');
            
            # Modo local
            s.sendall(b'SYSTem:LOCal\n');
        






        # Fragmento de codigo para el envio por MQTT a EMONCMS
        print("Envio de parametros por MQTT a EmonCMS");
        
        import time;
        import paho.mqtt.client as mqtt;
        
        # Definir los parametros de conexion
        broker_address = "37.187.27.45"
        broker_port = 1883    
        message = "433";
        
        # Callback cuando la conexion MQTT es establecida
        def on_connect(client, userdata, flags, rc):
            print("Conectado con el codigo de resultado: " + str(rc))
            client.publish("da3428cf56365aad/consumos_carga/potenciaProgramada", float(current_community_power)/1);  # Publicar el mensaje despues de la conexion
            
        # Crear un cliente MQTT
        client = mqtt.Client()
        
        # Configurar la funcion de conexion
        client.on_connect = on_connect
        
        
        # Establecer credenciales de conexion 
        client.username_pw_set(username="iotlibre", password="EeFfSytg33")
        
        # Conectar al broker MQTT
        client.connect(broker_address, broker_port, keepalive=60)
        
        # Mantener la conexion activa (puede ajustarse segun sea necesario)
        client.loop_start()
        
        # Esperar un momento para asegurar que el mensaje se haya enviado
        time.sleep(2)
        
        # Desconectar del broker MQTT
        client.disconnect()

        # Esperamos al siguiente ciclo
        time.sleep(60);

    # Fin del while
    
    
    
except KeyboardInterrupt:
    # Codigo a ejecutar cuando se presiona Ctrl+C
    print("\nSe ha presionado Ctrl+C. Ejecutando codigo de salida...")
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
    s.connect((ip_carga, pto_carga));
        
    # Modo remoto
    s.sendall(b'SYSTem:REMote\n');
            
    # Encender la carga despues de mandarle una consigna
    s.sendall(b'INPut 0\n');
            
    # Modo local
    s.sendall(b'SYSTem:LOCal\n');

finally:
    # Codigo a ejecutar siempre antes de salir, incluso si no hay excepcion
    print("Saliendo del programa.")
