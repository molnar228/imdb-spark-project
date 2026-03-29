import os
import sys
import platform
from pyspark.sql import SparkSession

# Імпортуємо всі наші модулі
from data_loader import get_imdb_dataframes
from data_visualizer import plot_movies_per_year, plot_ratings_distribution
from data_preprocessor import clean_basics, clean_ratings, print_statistics

# --- Налаштування середовища ---
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

spark = SparkSession.builder.appName("IMDB_Pipeline").getOrCreate()

try:
    # ВИДОБУВАННЯ ДАНИХ
    # print("\n--- ЕТАП 1: ВИДОБУВАННЯ ---")
    df_basics_raw, df_ratings_raw = get_imdb_dataframes(spark, DATA_DIR)

    # print("Перевірка Basics:")
    # df_basics_raw.show(5)

    #ВІЗУАЛІЗАЦІЯ

    # print("\n--- СТВОРЕННЯ ГРАФІКІВ ---")
    # plot_movies_per_year(df_basics_raw, OUT_DIR)
    # plot_ratings_distribution(df_ratings_raw, OUT_DIR)

    #ПОПЕРЕДНЯ ОБРОБКА
    print("\n--- ЕТАП 2: ПОПЕРЕДНЯ ОБРОБКА (Очищення) ---")

    df_basics_clean = clean_basics(df_basics_raw)
    df_ratings_clean = clean_ratings(df_ratings_raw)

    print_statistics(df_basics_clean, df_ratings_clean)

    print("\n--- ПЕРЕВІРКА ОЧИЩЕНИХ ДАНИХ BASICS ---")
    df_basics_clean.show(5)

    print("\nВсі операції завершено успішно!")

finally:
    spark.stop()