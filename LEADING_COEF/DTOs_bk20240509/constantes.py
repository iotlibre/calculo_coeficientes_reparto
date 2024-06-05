import datetime

def bisiesto(fecha):
    try :
        anyo = fecha.year
        fecha = datetime.date(anyo,2,29)
        Bisiesto = True
    except:
        Bisiesto = False
    
    return Bisiesto


def bisiestoA(anyo):
    try :
        fecha = datetime.date(anyo,2,29)
        Bisiesto = True
    except:
        Bisiesto = False
    
    return Bisiesto

ANYOELEGIDO = 2024

BLANCO = ""

ESPACIO = " "

ESPANOL = "es"

INGLES = "en"

INT = 1

SMALLINT = 2

STRING = 3

SI = "S"

NO = "N"

CORREO_INCID_ABIERTA_REGISTRADOR = "1"

CORREO_INCID_ABIERTA_RESPONSABLE = "2"

FORMATO_FECHA_MYSQL = "%Y-%m-%d %H:%M:%S"

FORMATO_FECHA_MYSQL_SIMPLE = "%Y-%m-%d"

FORMATO_FECHA_STD_SIMPLE = "%d/%m/%Y"

FORMATO_FECHA_STD_HORA_SEGUNDO_MINUTO = "%d/%m/%Y %H:%M:%S"

FORMATO_FECHA_STD_COMPUESTA = "%d/%m/%Y %H:%M"

FORMATO_HORA_STD_SIMPLE = "%H:%M"

FORMATO_HORA_CONSEGUNDOS = "%H:%M:%S"

NULL = "null"

GUION_BAJO = "_"

COD_SUPPLIER_NO_CLASIFICADO = "-1"

COD_SUPPLIER_PV = "2"

COD_SUPPLIER_CALIBRADOR = "1"

CORREO_ADMINISTRADOR = "fgregorio@isfoc.com"

CORREO_GOCPV = "gocpv@isfoc.com"

VALOR_NO_VALIDO = "-0"

DATASTATUS_OK = "0"

DATASTATUS_FILLED = "1"