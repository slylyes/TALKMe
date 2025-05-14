#!/bin/bash

# Enregistre l'heure de début en nanosecondes
start=$(date +%s.%N)

# Exécute la commande
curl --noproxy localhost --location 'http://localhost:8080/api/upload?tableName=Table&limite=100000' \
--header 'Content-Type: application/octet-stream' \
--data-binary '@/users/Etu5/21220655/Téléchargements/TALKMe-requete/data/yellow_tripdata_2009-01.parquet'


# Enregistre l'heure de fin
end=$(date +%s.%N)

# Calcule la durée
duration=$(echo "$end - $start" | bc)

# Affiche le temps écoulé
printf "\n Temps d'exécution : %.3f secondes\n" "$duration"
