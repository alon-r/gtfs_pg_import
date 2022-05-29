#!make
start_pg:
	@docker network create gtfs_local || true
	@docker run --name pg_gtfs_local --network gtfs_local -it --rm -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgis/postgis

stop_pg:
	@docker rm -f pg_gtfs_local

build:
	@docker build -t gtfs_import .

run_load_sources:
	@docker run --network gtfs_local -e GTFS_DB=postgresql://postgres:password@pg_gtfs_local/ gtfs_import load-sources

run_load_data:
	@docker run --network gtfs_local -e GTFS_DB=postgresql://postgres:password@pg_gtfs_local/ gtfs_import load-data

