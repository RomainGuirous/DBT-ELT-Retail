# DBT-ELT-Retail

pour lancer containers:

docker compose -f Docker/docker-compose.yml up --build -d

docker compose -f Docker/docker-compose.yml run --rm dbt dbt seed (pour lancer raw_ventes.csv)
docker compose -f Docker/docker-compose.yml run --rm dbt dbt run (lancer créer les tables dans models et y injecter les data de la seed)

http://localhost:3000 (se connecter à Metabase)