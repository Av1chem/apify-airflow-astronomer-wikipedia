FROM quay.io/astronomer/ap-airflow:2.1.1-buster-onbuild

ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV WIKI_CONFIG={"queries":["data collection","data extraction","ETL","Apify","Airflow"]}