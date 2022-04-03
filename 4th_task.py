"""Представь, что ты собрался снимать фильм и необходимо подобрать актерский состав.
Твоей задачей будет выбрать самых востребованных актеров, будем считать, что актер востребованный, если он снимался в топовых фильмах и не один раз"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, expr
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import concat_ws, col, year, current_date
from pyspark.sql.window import Window

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
top_100_all = res_df.limit(100)

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
raw_principle_df = principle_df.select("tconst", "nconst", "category").filter(
    col("category") == "actor"
)


top_actors = (
    raw_principle_df.join(name_df, "nconst", "inner")
    .join(top_100_all, "tconst", "inner")
    .groupBy("primaryName")
    .count()
)
res = (
    top_actors.select(top_actors["primaryName"])
    .filter(top_actors["count"] >= 2)
    .orderBy(top_actors["count"].desc())
    .drop(top_actors["count"])
)

res.coalesce(1).write.option("header", "true").option("inferSchema", "true").csv(
    "output/top_actors"
)
