import numpy as np
from DTOs.constantes import bisiestoA
from DTOs.constantes import ANYOELEGIDO as Anyo
# import accesos.AccesoCommons as AccesoCommons

#*
# Objeto DTO (data object transfer).
# 
# @author fgregorio
#
# @traductor jnaveiro
#

esBis = bisiestoA(Anyo)

if esBis:
    NUMDIAS = 366
else:
    NUMDIAS = 365

NUMHORAS = 24

class GeneradorEnergiaDTO:
    def __init__(self,idGeneradorEnergia="",dsGeneradorEnergia=""):
        self.idGeneradorEnergia = idGeneradorEnergia
        self.dsGeneradorEnergia = dsGeneradorEnergia
        self.Generacion = np.zeros((NUMDIAS,NUMHORAS))

    def getIdGeneradorEnergia(self):
        return str(self.idGeneradorEnergia)

    def setIdGeneradorEnergia(self, idGeneradorEnergia):
        self.idGeneradorEnergia = idGeneradorEnergia

    def getDsGeneradorEnergia(self):
        return str(self.dsGeneradorEnergia)

    def setDsGeneradorEnergia(self,dsGeneradorEnergia):
        self.dsGeneradorEnergia = dsGeneradorEnergia

    def getGeneracion(self):
        return self.Generacion

    def setGeneracion(self,Generacion):
        self.Generacion = Generacion
