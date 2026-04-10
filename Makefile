.PHONY: help init build up seed run test docs clean

# Exibe os comandos disponíveis
help:
	@echo ""
	@echo "  make init    — copia .env.example → .env e cria pasta credentials/"
	@echo "  make build   — constrói as imagens Docker sem rodar"
	@echo "  make up      — pipeline completo: carrega raw + executa dbt build"
	@echo "  make seed    — apenas carrega os dados fake no BigQuery (raw-loader)"
	@echo "  make run     — apenas dbt run (sem testes)"
	@echo "  make test    — apenas dbt test"
	@echo "  make docs    — gera e serve documentação dbt em localhost:8080"
	@echo "  make clean   — remove containers, volumes e artefatos compilados"
	@echo ""

# Copia o template de variáveis e cria a pasta para o service account
init:
	@[ -f .env ] || cp .env.example .env && echo ".env criado a partir de .env.example"
	@mkdir -p credentials
	@echo "Coloque seu service-account.json em ./credentials/"

# Constrói as imagens sem iniciar os serviços
build:
	docker-compose build

# Pipeline completo: raw → dbt build (run + test)
up:
	docker-compose up --build

# Apenas ingestão raw (sem dbt)
seed:
	docker-compose run --rm raw-loader

# Apenas transformações dbt (raw já deve estar carregado)
run:
	docker-compose run --rm dbt run

# Apenas testes dbt
test:
	docker-compose run --rm dbt test

# Gera documentação e serve em localhost:8080
docs:
	docker-compose run --rm --service-ports -p 8080:8080 dbt docs generate
	docker-compose run --rm --service-ports -p 8080:8080 dbt docs serve --port 8080

# Remove containers parados, volumes anônimos e artefatos
clean:
	docker-compose down -v
	rm -rf target/ dbt_packages/ logs/
