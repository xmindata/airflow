import pandas as pd
import joblib
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import mlflow
import os
from pyspark.ml.classification import RandomForestClassificationModel

def make_predictions(input_file_path, model_path, scaler_path, predict_file_path, mlflow_mode=False):
    # Create a SparkSession
    spark = SparkSession.builder.appName("IoTDeviceFailure").getOrCreate()

    # Load the scaler from a file
    pipeline_model = PipelineModel.load(scaler_path)

    # Load the new data from a CSV file
    new_data = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(input_file_path)

    # Apply the transformations to the new data
    new_data_scaled = pipeline_model.transform(new_data)

    # Check if we are using MLflow
    if mlflow_mode:
        # Load the run ID from the file
        with open(f'{model_path}/latest_run_id.txt', 'r') as f:
            run_id = f.read().strip()

        # Set MLFLOW_TRACKING_URI to the address of the MLflow tracking server
        os.environ['MLFLOW_TRACKING_URI'] = 'http://0.0.0.0:6000'

        # Load the model from MLflow
        model_uri = f'runs:/{run_id}/model'
        model = mlflow.spark.load_model(model_uri)
    else:
        # Load the model directly from a file
        model = RandomForestClassificationModel.load(model_path)

    # Make predictions
    predictions = model.transform(new_data_scaled)
    predictions.show()

    # Write the predictions to a CSV file
    predictions.select('prediction').write.format('csv').option('header', 'true').mode("overwrite").save(predict_file_path)
    print (f'Predictions have been written to {predict_file_path}')
    return