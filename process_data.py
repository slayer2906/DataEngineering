from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql.functions import substring_index, input_file_name, regexp_extract, from_unixtime, unix_timestamp, udf, col, lower
import configparser
from udf_functions import magnitude_udf

# Load schema configuration from schema.cfg
schema_config = configparser.ConfigParser()
schema_config.read('schema.cfg')

spark = SparkSession.builder \
    .appName("Merge and Process Small Files") \
    .getOrCreate()

# Load data
def load_data(input_path, schema):
    df = spark.read.format('csv').schema(schema).load(input_path)
    return df

# Add middle_value column
def add_middle_value_column(df):
    df = df.withColumn("middle_value", substring_index(substring_index("id", "-", 3), "-", -1))
    return df

# Format date column
def format_date_column(df, file_type):
    date_pattern = rf'{file_type}_(\d{{8}}_\d{{6}})\.txt'
    df = df.withColumn("date", regexp_extract(input_file_name(), date_pattern, 1))
    df = df.withColumn("formatted_date", from_unixtime(unix_timestamp("date", "yyyyMMdd_HHmmss"), "yyyy-MM-dd HH:mm:ss"))
    return df

# Select columns
def select_columns(df, columns):
    df = df.selectExpr(*columns)
    return df

def process_file(file_type):
    input_path = f"/home/developer2/animal_data/alldata/{file_type}_*"
    schema = eval(schema_config[file_type]['schema'])
    df = load_data(input_path, schema)
    df = add_middle_value_column(df)
    df = format_date_column(df, file_type)
    df = df.withColumn("size_int", df["size"].cast(IntegerType()))
    df = df.na.fill(-1, subset=["size_int"])
    df = df.withColumn("magnitude", magnitude_udf(col("size_int")))
    for column_name in df.columns:
        df = df.withColumn(column_name, lower(col(column_name)))
    columns_str = schema_config[f'sql_{file_type}']['columns']
    columns = [col_expr.strip() for col_expr in columns_str.split(',')]
    df = select_columns(df, columns)
    df = df.repartition(1)
    df.write.format('csv').option("header","true").option("delimiter",",").mode("overwrite").save(f'/home/developer2/animal_data/{file_type}.csv')
    return df
