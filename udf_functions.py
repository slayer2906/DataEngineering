# udf_functions.py
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def map_magnitude_udf(size):
    try:
        if size >= 500:
            return "massive"
        elif size >= 100:
            return "big"
        elif size >= 50:
            return "medium"
        elif size >= 10:
            return "small"
        elif size >= 1:
            return "tiny"
        else:
            return None
    except (ValueError, TypeError):
        return None

# Register the UDF
magnitude_udf = udf(map_magnitude_udf, StringType())
