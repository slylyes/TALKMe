#!/bin/bash

# Enregistre l'heure de début en nanosecondes
start=$(date +%s.%N)

# Exécute la commande
curl --noproxy localhost --location --request GET 'http://localhost:8080/data/filter' \
--header 'Content-Type: application/json' \
--data '{
    "name": "Table",
    "columns": [],
    "filters": [],
    "groupBy": [],
    "aggregates": []
}'
# Enregistre l'heure de fin
end=$(date +%s.%N)

# Calcule la durée
duration=$(echo "$end - $start" | bc)

# Affiche le temps écoulé
printf "\n Temps d'exécution : %.3f secondes\n" "$duration"
