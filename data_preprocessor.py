import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, BooleanType


def clean_basics(df_basics):
    print("--- Очищення таблиці Basics ---")

    df_cleaned = df_basics.drop("endYear", "originalTitle")

    df_cleaned = df_cleaned.withColumn(
        "startYear",
        F.when(F.col("startYear") == "\\N", None).otherwise(F.col("startYear")).cast(IntegerType())
    ).withColumn(
        "runtimeMinutes",
        F.when(F.col("runtimeMinutes") == "\\N", None).otherwise(F.col("runtimeMinutes")).cast(IntegerType())
    ).withColumn(
        "isAdult",
        F.when(F.col("isAdult") == "1", True).otherwise(False).cast(BooleanType())
    )


    df_cleaned = df_cleaned.dropDuplicates(["tconst"])
    df_cleaned = df_cleaned.dropna(subset=["startYear", "runtimeMinutes", "genres"])

    return df_cleaned


def clean_ratings(df_ratings):
    print("--- Очищення таблиці Ratings ---")

    df_cleaned = df_ratings.dropDuplicates(["tconst"])
    df_cleaned = df_cleaned.dropna(subset=["averageRating", "numVotes"])

    return df_cleaned


def print_statistics(df_basics, df_ratings):
    print("\n--- СТАТИСТИКА: TITLE.BASICS (Числові ознаки) ---")
    df_basics.select("startYear", "runtimeMinutes").summary("count", "mean", "min", "max").show()

    print("\n--- СТАТИСТИКА: TITLE.RATINGS (Числові ознаки) ---")
    df_ratings.select("averageRating", "numVotes").summary("count", "mean", "min", "25%", "50%", "75%", "max").show()