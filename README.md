# Airflow, MLFlow, and Spark
Practice for Airflow, MLflow and Spark.

## TO DO List
- [X] Initiate the virtual environment for local development
- [X] [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html) Installation
- [X] Airflow Configuration
- [ ] MLflow
- [ ] Pyspark
- [ ] Initiate the whole project for Azure development

## Set up the Airflow configuration

### Step 1: Create a Python virtual environment
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

### Step 2: Install/Upgrade [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html)

After installing the requirements.txt, run the following commands to update the airflow.
```
# install airflow
pip install apache-airflow

# or upgrade existing installation
pip install --upgrade apache-airflow
```

### Step 3: Set the AIRFLOW_HOME environment variable to your project directory

```
export AIRFLOW_HOME=$(pwd)
```
After this you can check it by 
```
echo $AIRFLOW_HOME
```

### Step 4: Initialize the database
Run this in the terminal to start the Airflow server.

```
airflow db init

airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org
```

This will initiate the Airflow database and create a user for you. 

### Step 5: Update the configuration file 

Open the `airflow.cfg` file in a text editor and update the `sql_alchemy_conn` and `executor` parameters to your desired settings. The `sql_alchemy_conn` parameter should point to the location of your database, and the `executor` parameter should be set to the type of `executor` you wish to use.

The `sql_alchemy_conn` shall be set up to the `/src/` folder where all the python scripts are located:
```
dags_folder = /absolute_dir/airflow/src/
```

**Update the executor to pick up the local python snippts**
```
dag_dir_list_interval = 30
```

After updating the configuration file, run the following command to upgrade the Airflow server.
```
airflow db upgrade
```

### Step 6: Start the Airflow server
Then, run the following command to start the Airflow server.
```
airflow webserver --daemon
airflow scheduler --daemon
```
This will make sure both the webserver and scheduler are running in the background.

### Step 7: Check the Airflow server
Now you can go to `http://localhost:8080/` to see the Airflow UI.

### Step 8: kill the Airflow server
In case the python code is updated, you need to kill the Airflow server and restart it again.

- Run the following command to kill the Airflow server ```ps aux | grep "airflow"```
- In order to do it in an easier way, copy the logging of and ask ChatGPT to kill the process for you.
- It shall follow a way such as ```kill 12345``` (12345 is the PID of the process)


## PySpark Configuration

To run Pyspark, you need to install Java and Spark first.

### Java Installation
Better just download from the official website, instead of using brew.

### Spark Installation

```
brew install apache-spark
```

