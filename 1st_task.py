"""Найди топ-100 фильмов(да-да, именно фильмов, а не сериалов или ТВ-шоу), заметь что фильмы с рейтингом 9,9 
и со 100 голосами мы не можем считать популярными, пусть если за фильм проголосовало хотя бы 100 000 человек
- он популярный за все время"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import year
from pyspark.sql.functions import current_date, col


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

res_df = (
    rating_df.join(title_df, rating_df.tconst == title_df.tconst, "inner")
    .select(
        rating_df["tconst"],
        title_df["primaryTitle"],
        rating_df["numVotes"],
        rating_df["averageRating"],
        title_df["startYear"],
    )
    .filter((title_df.titleType == "movie") & (rating_df.numVotes >= 100000))
    .sort(rating_df.averageRating.desc(), rating_df.numVotes.desc())
    .cache()
)
# за все время
top_100_all = res_df.select("*").limit(100)

top_100_all.coalesce(1).write.option("header", "true").option(
    "inferSchema", "true"
).csv("output/top100all")
# за последние 10 лет,
top100_10 = res_df.filter(col("startYear") >= (year(current_date()) - 10)).limit(100)
top100_10.coalesce(1).write.option("header", "true").option("inferSchema", "true").csv(
    "output/top100_10"
)
# фильмы которые были популярны в 60-х годах прошлого века.
top100_60 = res_df.filter(title_df.startYear.between(1960, 1969)).limit(100)
top100_60.coalesce(1).write.option("header", "true").option("inferSchema", "true").csv(
    "output/top100_60"
)
