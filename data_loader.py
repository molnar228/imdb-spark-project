from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def get_imdb_dataframes(spark, data_dir):


    basics_schema = StructType([
        StructField("tconst", StringType(), False),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", IntegerType(), True),
        StructField("startYear", StringType(), True),
        StructField("endYear", StringType(), True),
        StructField("runtimeMinutes", StringType(), True),
        StructField("genres", StringType(), True)
    ])

    ratings_schema = StructType([
        StructField("tconst", StringType(), False),
        StructField("averageRating", DoubleType(), True),
        StructField("numVotes", IntegerType(), True)
    ])


    df_basics = spark.read.csv(
        f"{data_dir}/title.basics.tsv",
        sep='\t',
        header=True,
        schema=basics_schema,
        nullValue='\\N'
    )

    df_ratings = spark.read.csv(
        f"{data_dir}/title.ratings.tsv",
        sep='\t',
        header=True,
        schema=ratings_schema,
        nullValue='\\N'
    )

    return df_basics, df_ratings