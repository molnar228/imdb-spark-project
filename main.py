import os
import sys
import platform
from pyspark.sql import SparkSession
from data_loader import get_imdb_dataframes
from data_visualizer import plot_movies_per_year, plot_ratings_distribution

if platform.system() == "Windows":
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['JAVA_HOME'] = r'C:\Progra~1\Java\jre1.8.0_51'
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    DATA_DIR = r"D:\educ\6term\data"
    OUT_DIR = "."
else:
    DATA_DIR = "/data"
    OUT_DIR = "/data"

spark = SparkSession.builder.appName("IMDB_Visualization").getOrCreate()

try:
    df_basics, df_ratings = get_imdb_dataframes(spark, DATA_DIR)

    print("\n--- ПЕРЕВІРКА ЗЧИТУВАННЯ BASICS ---")
    df_basics.show(5)

    print("\n--- ПЕРЕВІРКА ЗЧИТУВАННЯ RATINGS ---")
    df_ratings.show(5)

    print("\n--- ПОЧИНАЮ СТВОРЕННЯ ГРАФІКІВ ---")
    plot_movies_per_year(df_basics, OUT_DIR)
    plot_ratings_distribution(df_ratings, OUT_DIR)

    print("Всі операції завершено успішно!")

finally:
    spark.stop()