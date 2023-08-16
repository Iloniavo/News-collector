# News-collector

This is a simple project, with an ETL processus orchestrated with Apache Airflow in order to analyze the last breaking news in the world via different sources

__NB__ : 
- Don't forget to install all necessary modules to run the dag locally

```
pip install -r requirements.txt 

```
- To manually run the ETL pipeline without Apache Airflow, you can run the ***extractNewsApi.ipynb*** notebook file, and load your environment variables in your local project