import pandas as pd
import joblib
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel


def make_predictions(input_file_path, model_input_path, scaler_input_path, output_file_path):
    # Create a SparkSession
    spark = SparkSession.builder.appName("IoTDeviceFailure").getOrCreate()

    # Load the model and scaler from files
    model = PipelineModel.load(model_input_path)
    pipeline_model = PipelineModel.load(scaler_input_path)

    # Load the new data from a CSV file
    new_data = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(input_file_path)

    # Apply the transformations to the new data
    new_data_scaled = pipeline_model.transform(new_data)

    # Make predictions
    predictions = model.transform(new_data_scaled)

    # Write the predictions to a CSV file
    predictions.select('prediction').write.format('csv').option('header', 'true').save(output_file_path)
