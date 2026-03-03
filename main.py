import os
import sys
import platform

if platform.system() == "Windows":
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['JAVA_HOME'] = r'C:\Progra~1\Java\jre1.8.0_51'
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    print(">>> Запущено локально на Windows")
else:

    print(">>> Запущено всередині Docker-контейнера")

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IMDB_Project") \
    .getOrCreate()

print("--- СИСТЕМА ПРАЦЮЄ! Spark версії:", spark.version, "---")

spark.stop()