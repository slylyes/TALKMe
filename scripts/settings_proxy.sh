#!/bin/bash

# Définir le chemin du fichier
M2_DIR="$HOME/.m2"
SETTINGS_FILE="$M2_DIR/settings.xml"

# Vérifier si le fichier existe
if [ ! -f "$SETTINGS_FILE" ]; then
  echo "Création du fichier settings.xml dans $M2_DIR..."

  # Créer le répertoire ~/.m2 s'il n'existe pas
  mkdir -p "$M2_DIR"

  # Écrire le contenu dans le fichier
  cat <<EOF > "$SETTINGS_FILE"
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0">
    <proxies>
        <proxy>
            <id>proxy</id>
            <active>true</active>
            <protocol>http</protocol>
            <host>proxy</host>
            <port>3128</port>
            <username></username>
            <password></password>
            <nonProxyHosts></nonProxyHosts>
        </proxy>
    </proxies>
</settings>
EOF

  echo "Fichier créé avec succès."
else
  echo "Le fichier settings.xml existe déjà dans $M2_DIR. Aucune action effectuée."
fi