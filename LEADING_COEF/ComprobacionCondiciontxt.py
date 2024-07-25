import numpy as np

def compruebaNombre(nombre):
    try:
        lista = nombre[:-4].split("_")
        anyo = int(lista[-1])
        CAU = lista[0]
        return CAU,anyo
    
    except Exception as err:
        raise(err)

def lecturaArchivo(archivoLectura):
    with open(archivoLectura,"r") as o:
        contenido = o.readlines()
    return contenido

def coeficientesAgrup(contenido,horasAnyo):
    coef = []
    cups = []
    cupsAux = ""
    for i in contenido:
        lista = (i.strip("\n")).split(";")
        coef.append(float(lista[-1]))
        if cupsAux != lista[0]:
            cups.append(lista[0])
            cupsAux = lista[0]

    usuarios = len(coef)//horasAnyo
    if len(coef)%horasAnyo != 0:
        return None
    else:
        grupo = np.zeros((horasAnyo,usuarios))
        for j in range(horasAnyo):
            for k in range(usuarios):
                grupo[j,k] = coef[k*horasAnyo+j]
    
    return cups, grupo

def arregloSuma(grupoCoef):
    sumaDeCoef = np.sum(grupoCoef,axis=1)
    unos = np.ones(len(grupoCoef))
    matrizArreglo = unos - sumaDeCoef

    grupoCoef[:,0] += matrizArreglo

def escritura(nombreArchivo,cups,coef):
    with open(nombreArchivo,"w") as f:
        for i in range(len(cups)):
            for j in range(len(coef)):
                f.write("\n{:22s};{:4s};{:.6f}".format(cups[i],str(10001+j)[-4:],coef[j,i]).replace(".",","))

    g = open(nombreArchivo,"r")
    todo = g.readlines()
    g.close()

    with open(nombreArchivo,"w") as g:
           g.writelines(todo[1:])

if __name__ == "__main__":
    archivoLectura = "CAU33_2024.txt"
    archivoEscritura = "ES0026000010096279CP1FA000_2024.txt"
    nombre,anyo = compruebaNombre(archivoLectura)
    contenido = lecturaArchivo(archivoLectura)
    if anyo%4 == 0:
        horasAnyo = 24*366
    else:
        horasAnyo = 24*365

    try:
        cups,grupoCoef = coeficientesAgrup(contenido,horasAnyo)
        arregloSuma(grupoCoef)
        escritura(archivoEscritura,cups,grupoCoef)

    except:
        print("Número de horas no es exacto")
    
