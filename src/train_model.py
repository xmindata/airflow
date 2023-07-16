from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import mlflow
import os
import shutil

def train_model(input_train_data_path, input_test_data_path, model_path, scaler_path, mlflow_mode = False):
    # Create a SparkSession
    spark = SparkSession.builder.appName("IoTDeviceFailure").getOrCreate()

    # Load the training and test data from CSV files
    train_data = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(input_train_data_path)
    test_data = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(input_test_data_path)

    # # Load the pipeline model from a file
    pipeline_model = PipelineModel.load(scaler_path)

    # # Apply the transformations to the training and test data
    train_data_scaled = pipeline_model.transform(train_data)
    test_data_scaled = pipeline_model.transform(test_data)

    # Define the model
    model = RandomForestClassifier(labelCol='label', featuresCol='features', numTrees=100, maxDepth=2, seed=42)

    # Train the model
    model = model.fit(train_data_scaled)
    # Delete the existing model directory if it exists
    if os.path.exists(model_path):
        shutil.rmtree(model_path)

    # Save the model
    model.save(model_path)

    # Make predictions
    # Note that it's using transform() instead of predict() to do the predictions for spark ML
    predictions = model.transform(test_data_scaled)

    # Evaluate the model
    evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
    accuracy = evaluator.evaluate(predictions)


    if mlflow_mode:
        # Specify the MLflow server
        os.environ['MLFLOW_TRACKING_URI'] = 'http://0.0.0.0:6000'

        # Start an MLflow run and log parameters, metrics, and the model
        with mlflow.start_run() as run:
            mlflow.log_param('numTrees', 100)
            mlflow.log_param('maxDepth', 2)
            mlflow.log_metric('accuracy', accuracy)

            # Log the Spark ML model to MLflow
            mlflow.spark.log_model(model, "model")

        # Save the run ID to a file so the inference function can pick it up 
        with open(os.path.expanduser(f'{model_path}/latest_run_id.txt'), 'w') as f:
            f.write(run.info.run_id)