import os
import sys
import platform
from pyspark.sql import SparkSession

if platform.system() == "Windows":
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['JAVA_HOME'] = r'C:\Progra~1\Java\jre1.8.0_51'
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    print(">>> Запуск локально (Windows)")
else:
    print(">>> Запуск у контейнері (Docker)")

spark = SparkSession.builder \
    .appName("TestDataFrameApp") \
    .getOrCreate()

test_data = [
    ("The Shawshank Redemption", 1994, 9.3),
    ("The Godfather", 1972, 9.2),
    ("The Dark Knight", 2008, 9.0),
    ("Pulp Fiction", 1994, 8.9)
]

columns = ["Movie_Title", "Release_Year", "IMDB_Rating"]

df = spark.createDataFrame(test_data, schema=columns)

print("\n--- ТЕСТОВИЙ DATAFRAME ---")
df.show()

df.printSchema()

spark.stop()