# Airflow, MLFlow, and Spark
Practice for Airflow, MLflow and Spark.

## TO DO List
- [X] Initiate the virtual environment for local development
- [X] [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html) Installation
- [X] Airflow Configuration
- [ ] MLflow
- [ ] Pyspark
- [ ] Initiate the whole project for Azure development

## Initiate virtual environment

In the terminal, run the following command to initiate the virtual environment.

```
python3 -m venv .venv 
```

Then, run the following command to activate the virtual environment.

```
source .venv/bin/activate
```

Then install the requirements
```
pip install -r requirements.txt
```

The requirements are listed in the `requirements.txt` file.
```
apache-airflow==2.6.0
numpy==1.24.1
pandas==1.5.2
scipy==1.10.0 
mlflow==2.2.1
scikit-learn==1.2.1
pyspark==3.3.2 
findspark==2.0.1
```

## Initialization for [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html)

After installing the requirements.txt, run the following commands to update the airflow.
```
pip install --upgrade apache-airflow
```

Run this in the terminal to start the Airflow server.

```
airflow db init

airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org

airflow webserver --port 8080

airflow scheduler
```

Now you can go to `http://localhost:8080/` to see the Airflow UI.