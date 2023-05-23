# About the Repository

This repository contains the code to perform an ETL from a Postgres Database to a MySQL database using python and docker. 
This repository has 2 Branches to manage the WSL/Docker dependency for different CPU Vendors
- Master
-- This branch is for intel based system. It contains the dependencies that will work on Intel Based CPUs
- AMD:
-- This branch is for AMD based systems. It contains the dependencies that will work on AMD based CPUs.


## Running the docker

To get started run ``` docker-compose up ``` in root directory.
It will create the PostgresSQL database and start generating the data.
It will create an empty MySQL database.
It will launch the analytics.py script. 

Your task will be to write the ETL script inside the analytics/analytics.py file.