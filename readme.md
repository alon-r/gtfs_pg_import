
# GTFS Import
Python library for loading GTFS data into postgres (with postgis)  
Main source for GTFS data is OpenMobilityData (https://database.mobilitydata.org/)

## Install
Clone from GitHub

    git clone 

Install virtual env

    cd gtfs-import
    pip install virtualenv --user
    python3 -m venv venv
    . venv/bin/activate

Install gtfs-import 

    pip install --editable .

## Set Database connection
    export GTFS_DB=postgresql://[DB_USERNAME]:[DB_PASSWORD]@[DB_HOST]/

## Run Locally
Use local postgres on docker  

    make start_pg
    export GTFS_DB=postgresql://postgres:password@127.0.0.1/

## Download the sources URLs
Collect the sources list and store them in ```gtfs_feed_import``` table 

    gtfs_import load-sources

## Extract and load the Data to postgres
Download the sources listed in ```gtfs_feed_import``` and upload them to postgres

    gtfs_import load-data

## Running in docker
    make build
    make start_pg
    make run_load_sources
    make run_load_data
    
