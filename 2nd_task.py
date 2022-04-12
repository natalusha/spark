"""Лучшие фильмы нам известны, а что если ты захочешь посмотреть, ну, скажем лучший триллер всех времен и народов.Т
ак вот нужно найти по топ-10 фильмов каждого жанра (результат должен быть в одном файлике =))"""
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col, explode, split

spark = SparkSession.builder.appName("TotalOrdersPErRegionCountry").getOrCreate()
rating_file = "imdbdata/title.ratings.tsv/data.tsv"
rating_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .option("inferSchema", "true")
    .load(rating_file)
)
title_file = "imdbdata/title.basics.tsv/data.tsv"
title_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .option("inferSchema", "true")
    .load(title_file)
)

windowSpec = Window.partitionBy("genre").orderBy(
    rating_df["averageRating"].desc(), rating_df["numVotes"].desc()
)

top10genres = (
    rating_df.join(title_df, rating_df.tconst == title_df.tconst, "inner")
    .select(
        rating_df["tconst"],
        title_df["primaryTitle"],
        title_df["startYear"],
        (explode(split(title_df.genres, ",")).alias("genre")),
        rating_df["averageRating"],
        rating_df["numVotes"],
    )
    .filter((title_df.titleType == "movie") & (rating_df.numVotes > 100000))
    .withColumn("dense_rank", dense_rank().over(windowSpec))
    .sort("genre", "dense_rank")
)
res = top10genres.filter(col("dense_rank") <= 10).drop("dense_rank")
res.coalesce(1).write.option("header", "true").option("inferSchema", "true").csv(
    "output/top10genresNEW"
)
