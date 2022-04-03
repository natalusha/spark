"""Ну и напоследок, найди топ-5 фильмов по рейтингу у каждого режиссера"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, expr
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import concat_ws, col, year, current_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("TotalOrdersPErRegionCountry").getOrCreate()
title_file = "imdbdata/title.basics.tsv/data.tsv"
title_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .option("inferSchema", "true")
    .load(title_file)
)
rating_file = "imdbdata/title.ratings.tsv/data.tsv"
rating_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .option("inferSchema", "true")
    .load(rating_file)
)
name_file = "imdbdata/name.basics.tsv/data.tsv"
name_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .option("inferSchema", "true")
    .load(name_file)
)
principle_file = "imdbdata/title.principals.tsv/data.tsv"
principle_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .option("inferSchema", "true")
    .load(principle_file)
)
movie_df = (
    rating_df.join(title_df, "tconst", "inner")
    .select(
        rating_df["tconst"],
        title_df["primaryTitle"],
        rating_df["numVotes"],
        rating_df["averageRating"],
        title_df["startYear"],
    )
    .filter((title_df.titleType == "movie"))
    .sort(rating_df.averageRating.desc())
)

director_df = principle_df.select("tconst", "nconst", "category").filter(
    col("category") == "director"
)
movie_director_df = director_df.join(movie_df, "tconst", "inner").join(
    name_df, "nconst", "inner"
)

windowSpec = Window.partitionBy(movie_director_df["primaryName"]).orderBy(
    movie_director_df["averageRating"].desc()
)

dir_top5m = (
    movie_director_df.select(
        "primaryName", "primaryTitle", "startYear", "averageRating", "numVotes"
    )
    .withColumn("rank", row_number().over(windowSpec))
    .filter(col("rank") <= 5)
    .drop("rank")
)

dir_top5m.coalesce(1).write.option("header", "true").option("inferSchema", "true").csv(
    "output/dir_top5m"
)
