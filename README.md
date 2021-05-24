# Amazon Product Data DWH Loader 
  - [Summary](#summary)
  - [Quick start](#quick-start)
  - [Sample Reports from metabase](#sample-reports-from-metabase)
  - [Source files naming convention](#source-files-naming-convention)
  - [Data Model](#data-model)
  - [Scheduling](#scheduling)
## Summary
This application is designed to load [amazon product reviews](http://jmcauley.ucsd.edu/data/amazon/links.html) data set into a star schema for BI reporting and analytics. All you need to do is to download the source files and place them into a landing zone from where application will automatically pick them up for processing. As soon as the processing is done the star schema will be populated with dimension and fact data, and become available for reporting and analytics.

## Quick start
To start the application do following:
1. Download [source files](http://jmcauley.ucsd.edu/data/amazon/links.html) into your local machine.Make sure to download files from under the "Per-category files" section of the linked page. Both reviews and metadata files should be downloaded in order to process the given category. Example dataset pair per-category, [meta_Amazon_Instant_Video.json.gz](http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Amazon_Instant_Video.json.gz) & [reviews_Amazon_Instant_Video.json.gz](http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Amazon_Instant_Video.json.gz).
2. Make sure docker engine, docker compose and git installed.
3. Clone repository: `git clone https://github.com/javidy/reviews.git`.
4. Place source files (downloaded before) to landing_zone folder which will be under the project directory.
5. Start application with this command: `docker-compose up -d`. Application will start pulling docker images from [docker hub](https://hub.docker.com/). Just wait for a little while before images are downloaded and services become available. You can check availability at localhost:8084.


Once above steps preformed successfully, processing of the files will start automatically. Files will be processed by category, for instance, musical instruments category, baby category and so on. There is no order with which they will be processed. Interval between processing each category is 20 mins. If you want to check which files processed and their status you can query log table staging.reviews_execution_log from dwh db."success" in status column indicates that files processed successfully. Another place to check execution status is in airflow which will be at localhost:8084. DAG: reviews_dag

DWH details:
- DBMS: PostgreSQL
- Address: localhost:5432 (from host), dwh:5432 (from inside docker container)
- Database name: dwh
- Username: dwh_user
- Password: dwh_user
- Schema: dwh

Once all the data files loaded to star schema, you can connect to dwh with BI tool of your choice to do the reporting. You can also use [metabase](https://www.metabase.com/) for visualization and reporting which will be available at localhost:3000.

## DAG
Below is the screenshot of Airflow DAG for populating staging/dimension/fact tables. Dag has branch task for handling scenarios different scenarios such as files not found, one of the files missing , or happy path (files found). 
<img width="1428" alt="Screenshot 2021-05-24 at 21 24 36" src="https://user-images.githubusercontent.com/7736273/119397542-909a8100-bcd6-11eb-9f9d-ef9c474c5d62.png">

## Sample Reports from metabase
## Source files naming convention
## Data Model
## Scheduling

Image-based recommendations on styles and substitutes
J. McAuley, C. Targett, J. Shi, A. van den Hengel
SIGIR, 2015
[pdf](http://cseweb.ucsd.edu/~jmcauley/pdfs/sigir15.pdf)

Enjoy :)
