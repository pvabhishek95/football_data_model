up:
	docker compose --env-file env up --build -d

down:
	docker compose --env-file env down 

sh:
	docker exec -ti abhishek_app bash

run-etl:
	docker exec abhishek_app python data_modeling.py

warehouse:
	docker exec -ti warehouse psql postgres://sdeuser:sdepassword1234@localhost:5432/warehouse

