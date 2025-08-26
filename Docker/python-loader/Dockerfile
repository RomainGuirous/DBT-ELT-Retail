FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    # => Empêche Python d’écrire les fichiers .pyc (bytecode compilé) sur disque.
    # Utile en conteneur pour éviter des écritures inutiles dans l’image/volume et garder le système de fichiers propre.
    PYTHONUNBUFFERED=1
    # => Force la sortie standard et d’erreur à être non tamponnée (unbuffered).
    #Ça fait que les logs (print, erreurs) apparaissent immédiatement dans les logs Docker au lieu d’être retardés par le buffering.
    #Très pratique pour le debug et l’observation en temps réel.

# Installer dépendances système nécessaires pour psycopg, compilation éventuelle

RUN apt-get update \
    # met à jour la liste des paquets disponibles (index des dépôts).
    #Nécessaire avant un apt-get install pour s’assurer d’installer les dernières versions accessibles.
    && apt-get install -y --no-install-recommends \
    # -y: répond automatiquement "oui" aux prompts d’installation.
    # --no-install-recommends: n’installe pas les paquets recommandés, réduisant la taille de l’image.
       build-essential \
       # => outils de compilation (gcc, make) nécessaires si des bibliothèques Python doivent être compilées.
       libpq-dev \
       # => bibliothèques de développement PostgreSQL, nécessaires pour psycopg.
       ca-certificates \
       # => certificats CA, nécessaires pour les connexions HTTPS.
       curl \
       # => utilitaire pour télécharger des fichiers/faire des requêtes HTTP en ligne de commande.
    && rm -rf /var/lib/apt/lists/*
    # nettoie le cache apt pour réduire la taille de l’image. (inutile après l’installation)

WORKDIR /app

# Copier les dépendances
COPY requirements.txt /app/requirements.txt
# Installer les dépendances Python sans le cache pip pour réduire la taille de l’image
RUN pip install --no-cache-dir -r /app/requirements.txt

# Par défaut, garder un shell pour faciliter le debug; override via docker-compose command
CMD ["bash"]
