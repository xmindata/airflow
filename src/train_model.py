from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import mlflow

def train_model(input_train_data_path, input_test_data_path, output_file_path, scaler_input_path):
    # Create a SparkSession
    spark = SparkSession.builder.appName("IoTDeviceFailure").getOrCreate()

    # Load the training and test data from CSV files
    train_data = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(input_train_data_path)
    test_data = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(input_test_data_path)

    # Load the pipeline model from a file
    pipeline_model = PipelineModel.load(scaler_input_path)

    # Apply the transformations to the training and test data
    train_data_scaled = pipeline_model.transform(train_data)
    test_data_scaled = pipeline_model.transform(test_data)

    # Define the model
    model = RandomForestClassifier(labelCol='target', featuresCol='scaledFeatures', numTrees=100, maxDepth=2, seed=42)

    # Train the model
    model = model.fit(train_data_scaled)

    # Make predictions
    predictions = model.transform(test_data_scaled)

    # Evaluate the model
    evaluator = MulticlassClassificationEvaluator(labelCol='target', predictionCol='prediction', metricName='accuracy')
    accuracy = evaluator.evaluate(predictions)

    # Log the model to MLflow
    with mlflow.start_run():
        mlflow.log_param('numTrees', 100)
        mlflow.log_param('maxDepth', 2)
        mlflow.log_metric('accuracy', accuracy)
        mlflow.spark.log_model(model, "model")

    # Save the model to a file
    mlflow.spark.save_model(model, output_file_path)
