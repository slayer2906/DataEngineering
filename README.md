# DataEngineering
The contents of your project directory, DataEngineering-main, include the following files:

process_data.py: This file contains the main ETL functions or methods that process the data.
schema.cfg: This file contains the schema configurations for different data types, which are used to correctly load and process the data.
unit_test.py: This file contains the unit tests for the functions or methods defined in process_data.py.
udf_functions.py: This file contains user-defined functions (UDFs) used in the data processing.
main.py: This is the main entry point for running the ETL process. It likely uses functions or methods from process_data.py.
Now, let's proceed with the Functional Requirements Specification (FRS) based on these files and the previous discussions:

Functional Requirements Specification Document
1. Overview:
The system is a data processing pipeline developed in PySpark. It is designed to perform ETL (Extract, Transform, Load) operations on data files of different types. The types of data files it can process are 'apple_cat', 'apple_dog', 'banana_cat', and 'banana_dog'.

2. Detailed Requirements:
2.1 Data Extraction:
The system should be able to read data files from a specified location. The schema for each file type is defined in the schema.cfg file.

2.2 Data Transformation:
The system performs several transformations on the data:

Extracts a substring from the "id" field and adds it as a new column ("middle_value").
Extracts a date string from the file name and adds it as a new column ("date").
Formats the "date" field into a more standard date-time format.
Casts the "size" field to an integer and handles null values by replacing them with -1.
Applies a user-defined function ('magnitude_udf') to the "size_int" column and adds the result as a new column ("magnitude").
Converts all column values to lowercase.

2.3 Data Loading:
After the transformations, the system selects specific columns from the DataFrame based on SQL expressions defined in the schema.cfg file. Finally, it displays the DataFrame.

3. Testing:
The system includes unit tests (in unit_test.py) to verify that the data extraction, transformation, and loading functions work as expected.
These tests can be run independently of the main ETL process to check the correctness of the ETL functions. I havent added all the test due to time constraint.

5. User-Defined Functions:
The system uses a user-defined function ('magnitude_udf'), which is defined in udf_functions.py. This function is applied to the "size_int" column during the data transformation process.

6. Execution:
The main entry point for executing the ETL process is main.py. This script uses the functions from process_data.py to perform the ETL process for each file type.

7. Future Requirements/Improvements:
Error handling: The system should be able to handle errors that occur during the ETL process, such as missing files or incorrect schemas.
Logging: The system should log important events during the ETL process, such as the start and end times of each operation, to aid in debugging and performance optimization.




