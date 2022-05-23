#!make
start_pg:
	@docker run --name pg_gtfs_local -it --rm -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgis/postgis
stop_pg:
	@docker rm -f pg_gtfs_local