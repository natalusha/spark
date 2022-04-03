"""А теперь усложним задачу. Нужно найти все то же самое, но только для каждого десятилетия c сейчас до 1950х (тоже в одном файле) 
*нужно найти по топ-10 фильмов каждого жанра для каждого десятилетия c сейчас до 1950х"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, expr
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import concat_ws, col, year, current_date
from pyspark.sql.types import IntegerType
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
windowSpec = Window.partitionBy("genres", "yearRange").orderBy(
    rating_df["averageRating"].desc()
)
df = (
    rating_df.join(title_df, rating_df.tconst == title_df.tconst, "inner")
    .select(
        rating_df["tconst"],
        title_df["primaryTitle"],
        title_df["startYear"],
        title_df["genres"],
        rating_df["averageRating"],
        rating_df["numVotes"],
    )
    .filter(
        (title_df.titleType == "movie")
        & (rating_df.numVotes > 100000)
        & (title_df.startYear.between(1950, year(current_date())))
    )
    .sort(col("genres"), col("averageRating").desc())
)

nullyear_df = (
    df.withColumn("nullYear", expr("substring(startYear,1,3)||'0'").cast(IntegerType()))
    .withColumn("yearRange", concat_ws(" - ", col("nullYear"), col("nullYear") + 10))
    .withColumn("dense_rank", dense_rank().over(windowSpec))
    .filter(col("dense_rank") <= 10)
)
res = nullyear_df.drop("nullYear", "dense_rank")
res.coalesce(1).write.option("header", "true").option("inferSchema", "true").csv(
    "output/top10genresnow50s"
)
