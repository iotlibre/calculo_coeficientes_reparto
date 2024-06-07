import configparser
import os

def lecturaIni(archivo:str = "./LEADING_COEF/config_DB.ini"):
    direc = os.path.dirname(os.path.realpath(__file__))
    archivo2 = direc+"/"+ archivo
    config = configparser.ConfigParser()
    config.read(archivo2)

    user=config.get('Database_Server','user')
    password=config.get('Database_Server','password')
    host=config.get('Database_Server','host')
    port=int(config.get('Database_Server','port'))
    database=config.get('Database_Server','database')

    return user,password,host,port,database