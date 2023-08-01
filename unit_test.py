import unittest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row, StringType, DoubleType
from process_data import load_data, add_middle_value_column, format_date_column, select_columns

class TestETL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("ETLTest") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.data = [
            Row(id="7605c405-7ba9-40b0-a46e-b567d23af8b0", size="large", speed=5.5, jump_height=1.2),
            Row(id="7605c405-7ba9-40b0-a46e-b567d23af8b0", size="small", speed=7.8, jump_height=1.5)
        ]
        self.df = self.spark.createDataFrame(self.data)
        self.schema = "middle_value AS id, magnitude, speed, jump_height, formatted_date"


    def test_add_middle_value_column(self):
        df = add_middle_value_column(self.df)
        self.assertIn("middle_value", df.columns)
        row = df.first()
        self.assertEqual(row.middle_value, "40b0")

if __name__ == "__main__":
    unittest.main()
