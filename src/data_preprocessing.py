from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
import os

def process_data(input_file_path, output_train_data_path, output_test_data_path, scaler_output_path):
    # Create the directories if they do not already exist
    os.makedirs(output_train_data_path, exist_ok=True)
    os.makedirs(output_test_data_path, exist_ok=True)
    os.makedirs(scaler_output_path, exist_ok=True)
    
    # Create a SparkSession
    spark = SparkSession.builder.appName("IoTDeviceFailure").getOrCreate()

    # Load the processed data from a CSV file
    data = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(input_file_path)

    # Split the data into training and test sets
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

    # Define the feature columns
    feature_columns = [column for column in train_data.columns if column != 'target']

    # Define the transformations
    assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')
    scaler = StandardScaler(inputCol='features', outputCol='scaledFeatures', withStd=True, withMean=False)

    # Build the pipeline
    pipeline = Pipeline(stages=[assembler, scaler])

    # Fit the pipeline to the training data
    pipeline_model = pipeline.fit(train_data)

    # Transform the training and test data
    train_data_scaled = pipeline_model.transform(train_data)
    test_data_scaled = pipeline_model.transform(test_data)

    # Save the pipeline model to a file
    pipeline_model.save(scaler_output_path)

    # Write the processed data to new CSV files
    train_data_scaled.write.format('csv').option('header', 'true').save(output_train_data_path)
    test_data_scaled.write.format('csv').option('header', 'true').save(output_test_data_path)
