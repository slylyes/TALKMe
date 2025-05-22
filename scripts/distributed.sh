#!/bin/bash


if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <current_node_id> <node_count> <ip1> [<ip2> ...]"
  exit 1
fi

CURRENT_NODE_ID=$1
NODE_COUNT=$2
shift 2
NODE_IPS=("$@")

if [ $CURRENT_NODE_ID -gt $NODE_COUNT ]; then
  echo "Erreur : l'ID du noeud est supérieur au nombre de noeuds"
  exit 1
fi

if [ "${#NODE_IPS[@]}" -ne $(($NODE_COUNT - 1))  ]; then
  echo "Erreur : Le nombre d'IP fourni (${#NODE_IPS[@]}) ne correspond pas au nombre de noeuds ($NODE_COUNT)."
  exit 1
fi


# Détermination du chemin vers le fichier application.properties (depuis TALKMe/scripts)
FILE_PATH="../src/main/resources/application.properties"

# Commence la configuration
echo "# On definit la taille maximale du corps (taille du fichier donné) de la requete HTTP" > "$FILE_PATH"
echo "quarkus.http.limits.max-body-size=500M" >> "$FILE_PATH"
echo "" >> "$FILE_PATH"
echo "# Configuration for distributed query processing" >> "$FILE_PATH"
echo "distributed.enabled=true" >> "$FILE_PATH"
echo "distributed.node.count=$NODE_COUNT" >> "$FILE_PATH"
echo "" >> "$FILE_PATH"

echo "# IP addresses and ports for worker nodes" >> "$FILE_PATH"
echo "distributed.node.1.ip=localhost" >> "$FILE_PATH"
echo "distributed.node.1.port=8080" >> "$FILE_PATH"

# Ajouter les IP des noeuds
for i in "${!NODE_IPS[@]}"; do
  NODE_INDEX=$((i + 2))
  echo "distributed.node.$NODE_INDEX.ip=${NODE_IPS[$i]}" >> "$FILE_PATH"
  echo "distributed.node.$NODE_INDEX.port=8080" >> "$FILE_PATH"
done

echo "" >> "$FILE_PATH"

# Configuration de l'instance actuelle
echo "# Current node information" >> "$FILE_PATH"
echo "current.node.id=$CURRENT_NODE_ID" >> "$FILE_PATH"

# Si le noeud courant est différent de 1, on ajoute son IP
if [ "$CURRENT_NODE_ID" -ne 1 ]; then
  CURRENT_IP="${NODE_IPS[$((CURRENT_NODE_ID - 2))]}"
  echo "quarkus.http.host=$CURRENT_IP" >> "$FILE_PATH"
  echo "quarkus.http.port=8080" >> "$FILE_PATH"
fi



echo "Fichier $FILE_PATH généré avec succès."
