import os
import matplotlib.pyplot as plt
import pyspark.sql.functions as F


def plot_movies_per_year(df_basics, output_dir="."):
    print("Роблю розрахунки для графіка 'Фільми по роках'...")

    movies_year_df = df_basics.filter(
        (F.col("titleType") == "movie") &
        (F.col("startYear") != "\\N") &
        (F.col("startYear").isNotNull())
    ).groupBy("startYear").count()

    recent_movies = movies_year_df.filter(
        F.col("startYear").between("2000", "2023")
    ).orderBy("startYear")

    pdf = recent_movies.toPandas()

    plt.figure(figsize=(10, 5))
    plt.plot(pdf["startYear"], pdf["count"], marker='o', color='b', linewidth=2)
    plt.title("Кількість випущених фільмів (2000-2023)", fontsize=14)
    plt.xlabel("Рік")
    plt.ylabel("Кількість фільмів")
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()

    save_path = os.path.join(output_dir, "movies_per_year.png")
    plt.savefig(save_path)
    plt.close()
    print(f"Графік успішно збережено: {save_path}")


def plot_ratings_distribution(df_ratings, output_dir="."):
    print("Роблю розрахунки для графіка 'Розподіл оцінок'...")

    ratings_dist = df_ratings.withColumn(
        "rounded_rating", F.round(F.col("averageRating"))
    ).groupBy("rounded_rating").count().orderBy("rounded_rating")

    pdf = ratings_dist.toPandas()

    plt.figure(figsize=(10, 5))
    plt.bar(pdf["rounded_rating"], pdf["count"], color='orange', edgecolor='black')
    plt.title("Розподіл оцінок користувачів IMDB", fontsize=14)
    plt.xlabel("Оцінка (від 1 до 10)")
    plt.ylabel("Кількість фільмів/серіалів")
    plt.xticks(range(1, 11))
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    save_path = os.path.join(output_dir, "ratings_distribution.png")
    plt.savefig(save_path)
    plt.close()
    print(f"Графік успішно збережено: {save_path}")