#!/bin/bash

echo "Inicio sercio_comunidades"

while true
do

procesos=$(ps aux| grep LEADING | wc -l)
if [ $procesos -eq 1 ]; then
    python3 scripts_leading/LEADING_createCommunityUserByProfile_20240418.py
    echo "createCommunityUserByProfile"
fi
sleep 1

procesos=$(ps aux| grep LEADING | wc -l)
if [ $procesos -eq 1 ]; then
    python3 scripts_leading/LEADING_getEstimacionProduccion_PVGIS_20240417.py
    echo "getEstimacionProduccion"
fi
sleep 1

procesos=$(ps aux| grep LEADING | wc -l)
if [ $procesos -eq 1 ]; then
    python3 scripts_leading/LEADING_script_matriz_energ_baterias_20240418.py
    echo "script_matriz_energ_baterias"
fi
sleep 1

procesos=$(ps aux| grep LEADING | wc -l)
if [ $procesos -eq 1 ]; then
    python3 scripts_leading/LEADING_COEF/AEI_LEADING_CaracterizacionComunidadesEnergeticasTask_v6.py
    echo "CaracterizacionComunidadesEnergeticasTask"
fi
sleep 1

echo "FINAL bucle sercio_comunidades"
echo " "

done


