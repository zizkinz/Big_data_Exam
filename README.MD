docker compose up --build -d
docker compose exec ais-collision bash
spark-submit src/main.py