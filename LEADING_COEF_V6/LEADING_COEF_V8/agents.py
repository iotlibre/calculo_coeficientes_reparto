import mariadb
import sys
import os
from datetime import datetime as tiem
import logging
import configparser
from time import sleep

# *
#  Clase agente encargada de realizar las operaciones CRUD sobre base de datos.
#
#  @author fgregorio
#  @traductor jnaveiro
#
# /
SLEEPING_MS = 10/1000

class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some possible methods include: base class, decorator, metaclass. We will use the metaclass because it is best suited for this purpose.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

class MySqlAgent_AEI2022_LEADING(metaclass=SingletonMeta):
    """
    author: fgregorio

    traductor: jnaveiro

    Definición: Esta clase se encarga de la generación del agente de consulta a la base de datos. Su metaclase es SingletonMeta que evita que se instancie múltiples veces el mismo agente. Tiene varios métodos para trabajar con la base de datos en SQL.
    
    Constructor: Genera el agente con las credenciales propias de la base de datos
    
    Propiedades:

        1. conn
        2. cursor
    
    Métodos:

        1. MySqlAgent_AEI2022_LEADING.isValidConection
        2. MySqlAgent_AEI2022_LEADING.ejecutar
        3. MySqlAgent_AEI2022_LEADING.ejecutarMuchos
        4. MySqlAgent_AEI2022_LEADING.deleteByColumn
        5. MySqlAgent_AEI2022_LEADING.convertirFechaFormatoMySql
        6. MySqlAgent_AEI2022_LEADING.convertirFechaFormatoMySql
        7. MySqlAgent_AEI2022_LEADING.obtenerSecuenciaBySQL
        8. MySqlAgent_AEI2022_LEADING.getDataByProcedureLogin
        9. MySqlAgent_AEI2022_LEADING.deleteAlarm_procedure
        10. MySqlAgent_AEI2022_LEADING.setDataByProcedure
        11. MySqlAgent_AEI2022_LEADING.executeUploadDataFileCSV_procedure
        12. MySqlAgent_AEI2022_LEADING.initTransaction
        13. MySqlAgent_AEI2022_LEADING.commitTransaction
        14. MySqlAgent_AEI2022_LEADING.rollBackTransaction
        15. MySqlAgent_AEI2022_LEADING.setActionExecuted
        16. MySqlAgent_AEI2022_LEADING.getLastDataByDevice
        """

    def __init__(self, archivo:str = "config_DB.ini"):
        """
        Definición: Constructor. Declaramos la instancia del agente y de la conexión. Para ello hay que escribir las credenciales de acceso.
        
        Variables de entrada: Ninguna

        Variables de salida: Ninguna

        Propiedades modificadas:

            1. conn
            2. cursor
        """
        
        direc = os.path.dirname(os.path.realpath(__file__))
        archivo2 = direc+"\\"+ archivo
        config = configparser.ConfigParser()
        config.read(archivo2)

        # Connect to MariaDB Platform
        try:
            self.conn = mariadb.connect(
                            user=config.get('Database_Server','user'),
                            password=config.get('Database_Server','password'),
                            host=config.get('Database_Server','host'),
                            port=int(config.get('Database_Server','port')),
                            database=config.get('Database_Server','database'))
            self.cursor = self.conn.cursor()
            print("conexión realizada")

        except mariadb.Error as e:
            logging.error(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

    def isValidConection(self) :
        """
        Definición: Método para comprobar que la conexión se ha realizado correctamente.
        
        Variables de entrada: Ninguna
        
        Variables de salida: validConection (bool)
        
        Propiedades modificadas: Ninguna
        """
        # Declaramos la variable a devolver
        validConection = False

        # Realizamos una prueba para comprobar que la conexión es valida, sino lo indicaremos.
        sleep(SLEEPING_MS)
        try :
            # st = instanciaAgente.cursor.createStatement()
            st = MySqlAgent_AEI2022_LEADING()
            sleep(SLEEPING_MS)
            st.ejecutar("SELECT 1 FROM DUAL")
            validConection = True
            # st.cursor.close()
        except Exception as e:
            validConection = False
        

        # Devolvemos el resultado de la prueba
        return validConection

    def ejecutar(self,sql):
        """
        Definición: Ejecuta la sentencia SQL. En caso de poder devolver algún resultado de una consulta lo hace. Sino, ejecuta la sentencia y en caso de no poder ejecutarla devuelve un error en el archivo log.
        
        Variables de entrada: sql (str)
        
        Variables de salida: devuelve (list)
        
        Propiedades modificadas: cursor
        
        """
        try :
            # Ejecutamos la sentencia
            self.cursor.execute(sql)
            
            try:
                #En caso de devolver algún resultado lo devuelve todos como una lista de valores
                devuelve= self.cursor.fetchall()
                # Devolvemos el relleno
                return devuelve
            except:
                return

        except Exception as e :
            mensaje = "\tError en el MySqlAgent: MySqlAgent.execute()"
            sleep(SLEEPING_MS)
            logging.exception(mensaje+str(e))
            raise Exception(mensaje, e)

    def ejecutarMuchos(self,sql,listaarg):
        """
        author: jnaveiro
        
        Definición: Metodo encargado de ejecutar la sentencia sql pasada por argumentos. Se le deben pasar los argumentos en una lista de tuplas.
        
        Variables de entrada: sql (str), listaarg (list)
        
        Variables de salida: devuelve (list)
        
        Propiedades modificadas: cursor
        
        """
        try :

            # Ejecutamos la instancia
            self.cursor.executemany(sql,listaarg)
            try:
                #En caso de devolver algún resultado lo devuelve todos como una lista de valores
                devuelve=  self.cursor.fetchall()
                # Devolvemos
                return devuelve
            except:
                return

        except Exception as e :
            mensaje = "\tError en el MySqlAgent: MySqlAgent.executemany()"
            logging.exception(mensaje+str(e))
            raise Exception(mensaje, e)

    def  deleteByColumn(self, tabla, idColumn, idValue):
        """
        Definición: Método encargado de eliminar una tabla filtrando por losvalores de las columnas dadas por parámetros.
        
        Variables de entrada: tabla (str), idColumn(list), idValue(list)
        
        Variables de salida: Ninguna
        
        Propiedades modificadas: Ninguna
        
        """

        try :

            # Componemos la sentencia sql a ejecutar
            sqlDelete = "DELETE FROM " + tabla
            for i in range(len(idColumn)):
                if (i == 0) :
                    sqlDelete = sqlDelete + " WHERE " + idColumn[i] + "= " + idValue[i]
                else :
                    sqlDelete = sqlDelete + " AND " + idColumn[i] + "= " + idValue[i]

            # Ejecutamos la sentencia
            self.ejecutar(sqlDelete)

        except Exception as e :
            mensaje = "Error en el MySqlAgent: MySqlAgent.deleteByColumn()"
            raise Exception(mensaje, e)

    def convertirFechaFormatoMySql(self, sFecha, sHora):
        """
        Definición: Método encargado de convertir una cadena al formato necesario para los insert.
        
        Variables de entrada:  sFecha(str), sHora(str)
        
        Variables de salida: dfIBM (str)
        
        Propiedades modificadas: Ninguna
        
        """
        df = "%d/%m/%Y %H:%M:%S"
        date = tiem.strptime((sFecha + " " + sHora),df)
        dfIBM = date.__format__("%Y-%m-%d %H:%M:%S")
        return dfIBM

    def convertirFechaFormatoMySql(self,date):
        """
        Definición: Método encargado de convertir un date en una cadena para guardar como fecha.
        
        Variables de entrada: date(datetime)
        
        Variables de salida: fecha(str)
        
        Propiedades modificadas: Ninguna
        
        """
        # Convertimos el date al una cadena con formato a cadena db2.
        dfDate = "%d/%m/%Y"
        dfHour = "%H:%M:%S"
        return self.convertirFechaFormatoMySql(date.__format__(dfDate), date.__format__(dfHour))
    
    def obtenerSecuenciaBySQL(self,sql):
        """
        Definición: Método encargado de obtener la secuencia correspondiente para registrar una incidencia.
        
        Variables de entrada: sql(str)
        
        Variables de salida: seqDevolver(list)
        
        Propiedades modificadas: Ninguna
        
        """
        try :

            # Declaramos el listado a devolver
            seqDevolver = 1

            # Preparamos la instancia de la conexión
            st = self.cursor

            # Ejecutamos la consulta
            resultSet = st.execute(sql)

            # Devolvemos los datos tratados
            for valores in resultSet.fetchall():
                # Declaramos el objeto a devolver en el listado
                seqDevolver = valores[0]
                if (seqDevolver == 0):
                    seqDevolver = 1

            # Devolvemos la secuencia
            return seqDevolver

        except Exception as e :
            mensaje = "\tError en el MySqlAgent: MySqlAgent.obtenerSecuenciaBySQL()"
            raise Exception(mensaje, e)

    def getDataByProcedureLogin(self, login, password):
        """
        Definición: Método encargado de autentificar en base de datos el login de la aplicación.
        
        Variables de entrada: login(str), password(str)
        
        Variables de salida: seqDevolver(list)
        
        Propiedades modificadas: Ninguna
        
        """
        sleep(SLEEPING_MS)

        try :

            # Declaramos el listado a devolver
            cs = None

            # Ejecutamos la sentencia
            st = MySqlAgent_AEI2022_LEADING()
            sleep(SLEEPING_MS)
            cs = self.cursor.execute("CALL PRO_CHE_USER(?,?,?,?,?,?,?)",(login,password,bool,int,str,str,str))

            st.close()

            # Devolvemos el listado relleno
            return cs.fetchall()

        except Exception as e :
            mensaje = "\tError en el MySqlAgent: MySqlAgent.getDataByProcedureLogin()"
            raise Exception(mensaje, e)

    def deleteAlarm_procedure(self, idAlarm, idConsumption):
        
        """
        Definición: Método encargado de autentificar en base de datos el login de la aplicación.
        
        Variables de entrada: idAlarm(str), idConsumption(str)
        
        Variables de salida: seqDevolver(list)
        
        Propiedades modificadas: Ninguna
        
        """
        sleep(SLEEPING_MS)

        try :

            # Declaramos el listado a devolver
            cs = None

            # Ejecutamos la sentencia
            st = MySqlAgent_AEI2022_LEADING()
            sleep(SLEEPING_MS)
            cs = self.cursor.execute("CALL DDL_WEE_D_ALERT(?,?)",(idAlarm,idConsumption))

            st.close()

            # Devolvemos el listado relleno
            return cs.fetchall()

        except Exception as e :
            mensaje = "\tError en el MySqlAgent: MySqlAgent.deleteAlarm_procedure()"
            raise Exception(mensaje, e)

    def setDataByProcedure(self, fecha, idDevice, value):
        """
        Definición: Método encargado de autentificar en base de datos el login de la aplicación.
        
        Variables de entrada:  fecha(str), idDevice(str), value(str)
        
        Variables de salida: seqDevolver(list)
        
        Propiedades modificadas: Ninguna
        
        """

        sleep(SLEEPING_MS)
        
        try :

            # Declaramos el listado a devolver
            cs = None

            # Ejecutamos la sentencia
            st = MySqlAgent_AEI2022_LEADING()
            sleep(SLEEPING_MS)
            cs = self.cursor.execute("CALL DDL_WEE_F_ROWDATA_INUP(?,?,?)",(fecha,idDevice,value))

            st.close()

            # Devolvemos el listado relleno
            return cs.fetchall()

        except Exception as e :
            mensaje = "\tError en el MySqlAgent: MySqlAgent.setDataByProcedure()"
            raise Exception(mensaje, e)

    def executeUploadDataFileCSV_procedure (self, idUser):
        """
        Definición: Método encargado de autentificar en base de datos el login de la aplicación.
        
        Variables de entrada:  fecha(str), idDevice(str), value(str)
        
        Variables de salida: seqDevolver(list)
        
        Propiedades modificadas: Ninguna
        
        """
        try :

            # Declaramos el listado a devolver
            cs = None

            # Ejecutamos la sentencia
            st = MySqlAgent_AEI2022_LEADING()
            sleep(SLEEPING_MS)
            cs = self.cursor.execute("CALL LOADDATA_WEE_F_FILEDATAENERBOX(?)",(idUser))
            # Cerramos la conexión

            st.close()

            # Devolvemos el listado relleno
            return cs.fetchall()

        except Exception as e :
            mensaje = "\tError en el MySqlAgent: MySqlAgent.executeUploadDataFileCSV_procedure()"
            raise Exception(mensaje, e)

    def  initTransaction(self):
        """
        Definición: Método encargado de iniciar la transacción
        
        Variables de entrada: Ninguna
        
        Variables de salida: Ninguna
        
        Propiedades modificadas: Ninguna
        
        """
        sleep(SLEEPING_MS)

        try :
            
            # Obtenemos el agente y establecemos que el commit se realizará manualmente
            agenteCRUD = MySqlAgent_AEI2022_LEADING()
            sleep(SLEEPING_MS)
            agenteCRUD.cursor.autocommit = False
            
            agenteCRUD.close()

        except Exception as e :
            mensaje = "\tError en el MySqlAgent: MySqlAgent.initTransaction()"
            raise Exception(mensaje, e)

    def  commitTransaction(self):
        """
        Definición: Método encargado de realizar el commit de la transacción.
        
        Variables de entrada: Ninguna
        
        Variables de salida: Ninguna
        
        Propiedades modificadas: Ninguna
        
        """
        sleep(SLEEPING_MS)

        try :
            
            # Obtenemos la instancia del agente y realizamos el commit
            self.conn.commit()

            # Establecemos de nuevo el autocommit
            self.conn.autocommit = True

        except Exception as e :
            mensaje = "\tError en el MySqlAgent: MySqlAgent.commitTransaction()"
            raise Exception(mensaje, e)

    def  rollBackTransaction(self):
        """
        Definición: Método encargado de realizar el rollback de la transacción.
        
        Variables de entrada: Ninguna
        
        Variables de salida: Ninguna
        
        Propiedades modificadas: Ninguna
        
        """
        sleep(SLEEPING_MS)

        try :
            
            # Obtenemos la instancia del agente y realizamos el commit
            self.cursor.rollback()
            sleep(SLEEPING_MS)

            # Establecemos de nuevo el autocommit
            self.cursor.autocommit = True
            
        except Exception as e :
            mensaje = "\tError en el MySqlAgent: MySqlAgent.rollBackTransaction()"
            raise Exception(mensaje, e)

    def setActionExecuted(self, idSession, idFormAction, url):
        """
        Definición: Método encargado de autentificar en base de datos el login de la aplicación.
        
        Variables de entrada: Ninguna
        
        Variables de salida: Ninguna
        
        Propiedades modificadas: Ninguna
        
        """
        sleep(SLEEPING_MS)

        try :

            # Declaramos el listado a devolver
            cs = None

            # Ejecutamos la sentencia
            st = MySqlAgent_AEI2022_LEADING()
            sleep(SLEEPING_MS)
            cs = self.cursor.execute("CALL PCA_PFP.PCA_PRO_INSERT_PCA_M_SESSIONACCESS(?,?,?)",(idSession,idFormAction,url))

            st.close()

            # Devolvemos el listado relleno
            return cs.fetchall()

        except Exception as e :
            mensaje = "\tError en el MySqlAgent: MySqlAgent.setActionExecuted()"
            raise Exception(mensaje, e)

    def getLastDataByDevice(self,idDevice):
        """
        Definición: Método encargado de autentificar en base de datos el login de la aplicación.
        
        Variables de entrada: Ninguna
        
        Variables de salida: Ninguna
        
        Propiedades modificadas: Ninguna
        
        """
        sleep(SLEEPING_MS)

        try :

            # Declaramos el listado a devolver
            cs = None

            # Ejecutamos la sentencia
            st = MySqlAgent_AEI2022_LEADING()
            sleep(SLEEPING_MS)
            cs = self.cursor.execute("CALL PCA_PFP.PCA_PRO_GET_DEVICEDDAYLAST(?,?)",(int(idDevice),str))

            st.close()

            # Devolvemos el listado relleno
            return cs.fetchall()

        except Exception as e :
            mensaje = "\tError en el MySqlAgent: MySqlAgent.getLastDataByDevice()"
            raise Exception(mensaje, e)

