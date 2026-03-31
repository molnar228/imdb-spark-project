import pyspark.sql.functions as F
from pyspark.sql.window import Window
import os


def execute_business_queries(df_basics, df_ratings, output_dir="."):
    print("\n" + "="*50)
    print("ЕТАП ТРАНСФОРМАЦІЇ: ВИКОНАННЯ БІЗНЕС-ПИТАНЬ")
    print("="*50)

    # ПИТАННЯ 1 (Filter, Join)
    print("\n--- Питання 1: Топ-10 сучасних хітів (після 2010, рейтинг > 8.0) ---")
    q1 = df_basics.filter((F.col("titleType") == "movie") & (F.col("startYear") > 2010)) \
                  .join(df_ratings, "tconst") \
                  .filter(F.col("averageRating") > 8.0) \
                  .orderBy(F.desc("numVotes")) \
                  .limit(10)
    q1.select("primaryTitle", "startYear", "averageRating", "numVotes").show(truncate=False)
    q1.explain()

    # ПИТАННЯ 2 (Filter, GroupBy)
    print("\n--- Питання 2: Тренд тривалості фільмів по роках (з 2000) ---")
    q2 = df_basics.filter((F.col("titleType") == "movie") & (F.col("startYear") >= 2000)) \
                  .groupBy("startYear") \
                  .agg(F.count("*").alias("total_movies"), 
                       F.round(F.avg("runtimeMinutes"), 1).alias("avg_runtime")) \
                  .orderBy("startYear")
    q2.show(5)
    q2.explain()

    # ПИТАННЯ 3 (Filter, Join, Window)
    print("\n--- Питання 3: Фільм року (2015-2023) ---")
    window_spec_q3 = Window.partitionBy("startYear").orderBy(F.desc("averageRating"))
    q3 = df_basics.join(df_ratings, "tconst") \
                  .filter((F.col("titleType") == "movie") & 
                          (F.col("numVotes") > 50000) & 
                          (F.col("startYear").between(2015, 2023))) \
                  .withColumn("rank", F.rank().over(window_spec_q3)) \
                  .filter(F.col("rank") == 1) \
                  .orderBy(F.desc("startYear"))
    q3.select("startYear", "primaryTitle", "averageRating", "numVotes").show(truncate=False)
    q3.explain()

    # ПИТАННЯ 4 (Filter, Join, GroupBy)
    print("\n--- Питання 4: Топ-5 найуспішніших жанрів (від 10k голосів) ---")
    q4 = df_basics.join(df_ratings, "tconst") \
                  .filter((F.col("titleType") == "movie") & (F.col("numVotes") > 10000)) \
                  .groupBy("genres") \
                  .agg(F.round(F.avg("averageRating"), 2).alias("avg_rating"), 
                       F.count("*").alias("movie_count")) \
                  .filter(F.col("movie_count") > 50) \
                  .orderBy(F.desc("avg_rating")) \
                  .limit(5)
    q4.show(truncate=False)
    q4.explain()

    # ПИТАННЯ 5 (Filter, Join, Window)
    print("\n--- Питання 5: Різниця з середнім по жанру за 2023 рік ---")
    window_spec_q5 = Window.partitionBy("genres")
    q5 = df_basics.join(df_ratings, "tconst") \
                  .filter((F.col("startYear") == 2023) & 
                          (F.col("numVotes") > 10000) & 
                          (F.col("titleType") == "movie")) \
                  .withColumn("genre_avg", F.round(F.avg("averageRating").over(window_spec_q5), 2)) \
                  .withColumn("rating_diff", F.round(F.col("averageRating") - F.col("genre_avg"), 2)) \
                  .orderBy(F.desc("rating_diff")) \
                  .limit(10)
    q5.select("primaryTitle", "genres", "averageRating", "genre_avg", "rating_diff").show(truncate=False)
    q5.explain()

    # ПИТАННЯ 6 (Filter)
    print("\n--- Питання 6: Довгі документалки до 2000 року ---")
    q6 = df_basics.filter((F.col("titleType") == "movie") & 
                          (F.col("genres").contains("Documentary")) & 
                          (F.col("runtimeMinutes") > 120) & 
                          (F.col("startYear") < 2000)) \
                  .orderBy(F.desc("runtimeMinutes")) \
                  .limit(5)
    q6.select("primaryTitle", "startYear", "runtimeMinutes", "genres").show(truncate=False)
    q6.explain()

    # запису результатів
    print("\n" + "=" * 50)
    print("ЗБЕРЕЖЕННЯ РЕЗУЛЬТАТІВ У ФОРМАТІ CSV")
    print("=" * 50)

    results_dir = os.path.join(output_dir, "results")

    q1.select("primaryTitle", "startYear", "averageRating", "numVotes") \
        .coalesce(1).write.csv(os.path.join(results_dir, "q1_top_movies"), header=True, mode="overwrite")

    q2.coalesce(1).write.csv(os.path.join(results_dir, "q2_runtime_trend"), header=True, mode="overwrite")

    q3.select("startYear", "primaryTitle", "averageRating", "numVotes") \
        .coalesce(1).write.csv(os.path.join(results_dir, "q3_movie_of_the_year"), header=True, mode="overwrite")

    q4.coalesce(1).write.csv(os.path.join(results_dir, "q4_top_genres"), header=True, mode="overwrite")

    q5.select("primaryTitle", "genres", "averageRating", "genre_avg", "rating_diff") \
        .coalesce(1).write.csv(os.path.join(results_dir, "q5_genre_diff"), header=True, mode="overwrite")

    q6.select("primaryTitle", "startYear", "runtimeMinutes", "genres") \
        .coalesce(1).write.csv(os.path.join(results_dir, "q6_documentaries"), header=True, mode="overwrite")

    print(f"Файли успішно збережено у папку: {results_dir}")