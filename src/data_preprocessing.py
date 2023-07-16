from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.sql.functions import trim, concat_ws, unix_timestamp, hour, minute, log
from pyspark.ml import Pipeline
import os

def process_data(input_file_path, output_train_data_path, output_test_data_path, scaler_path):
    # Create the directories if they do not already exist
    os.makedirs(output_train_data_path, exist_ok=True)
    os.makedirs(output_test_data_path, exist_ok=True)
    os.makedirs(scaler_path, exist_ok=True)

    # Create a SparkSession
    spark = SparkSession.builder.appName("IoTDeviceFailure").getOrCreate()

    # Load the processed data from a CSV file
    data = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(input_file_path)
    print ('Data schema before the preprocessing:')
    data.printSchema()

    # Let's assume that your data has categorical and numerical columns. Replace these lists with your actual column names
    categorical_columns = ['temp_condition', 'type', 'label']
    numerical_columns = ['fridge_temperature']

    # Trim leading and trailing spaces
    for column in categorical_columns:
        data = data.withColumn(column, trim(data[column]))

    # Process datetime/date/time columns
    # Combine 'date' and 'time' into a single timestamp column
    data = data.withColumn("timestamp", unix_timestamp(concat_ws(' ', 'date', 'time'), 'MM/dd/yyyy HH:mm:ss').cast("timestamp"))
    # Extract hour and minute from 'timestamp' and add them as new columns
    data = data.withColumn("hour", hour("timestamp"))
    data = data.withColumn("minute", minute("timestamp"))
    # Update categorical_columns and numerical_columns to include the new columns

    # Logarithmic transformation for 'fridge_temperature'
    data = data.withColumn('fridge_temperature', log(data['fridge_temperature']))

    # Apply string indexing to categorical variables
    indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(data) for column in categorical_columns]
    for indexer in indexers:
        print(f"Column: {indexer.getInputCol()}")
        print(f"Unique values: {indexer.labels}")
        print("\n")

    # Split the data into training and test sets
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
    # Write the processed data to new CSV files
    train_data.write.mode('overwrite').format('csv').option('header', 'true').save(output_train_data_path)
    test_data.write.mode('overwrite').format('csv').option('header', 'true').save(output_test_data_path)
 
    # Define the feature columns
    feature_columns = [column for column in train_data.columns if column != 'label']

    # Replace original columns with indexed ones in feature columns
    for col in categorical_columns:
        feature_columns = [f'{col}_index' if c == col else c for c in feature_columns]

    # Define the transformations
    assembler = VectorAssembler(inputCols=['fridge_temperature'], outputCol="features")
    # scaler = StandardScaler(inputCol='features', outputCol='scaledFeatures', withStd=True, withMean=False)

    print ('Data schema after the preprocessing:')
    data.printSchema()

    # Build the pipeline
    pipeline = Pipeline(stages=indexers + [assembler])

    # Fit the pipeline to the training data
    pipeline_model = pipeline.fit(train_data)

    # Save the pipeline model to a file, overwrite if it already exists
    pipeline_model.write().overwrite().save(scaler_path)

    train_data.show()
    # Write the processed data to new Parquet files, overwrite if they already exist
    # train_data_scaled.write.mode('overwrite').parquet(output_train_data_path)
    # test_data_scaled.write.mode('overwrite').parquet(output_test_data_path)
