# Import and start spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("First PySpark Session").master("local[*]").getOrCreate()

#Read the CSV file into dataFrame
titles_df = spark.read.option("header", True).csv("C:/Users/delis/Downloads/raw_titles.csv")
titles_df.show()
titles_df.printSchema()

credits_df = spark.read.option("header", True).csv("C:/Users/delis/Downloads/raw_credits.csv")
credits_df.show()
credits_df.printSchema()

# Analyze the Top rated Movie and the actor that acted in it based on the year
top_rated_movie = credits_df.join(titles_df, "id") \
                         .filter((credits_df.role == "ACTOR") & (titles_df.imdb_score.isNotNull()) & (titles_df.type == "MOVIE")) \
                         .select("name", "role", "title", "type", "release_year", "imdb_score") \
                         .withColumn("Rank", row_number().over(Window.partitionBy("release_year").orderBy("imdb_score"))) \
                         .orderBy("release_year", "Rank")

top_rated_movie_list = top_rated_movie.filter("Rank < 2").orderBy("release_year", "Rank")
top_rated_movie_list.show(20)

# Analyze the Top rated Show and the actor that acted in it based on the year
top_rated_show = credits_df.join(titles_df, "id") \
                       .filter((credits_df.role == "ACTOR") & (titles_df.imdb_score.isNotNull()) & (titles_df.type == "SHOW")) \
                       .select("name", "role", "title", "type", "release_year", "imdb_score") \
                       .withColumn("Rank", row_number().over(Window.partitionBy("release_year").orderBy("imdb_score"))) \
                       .orderBy("release_year", "Rank")

top_rated_show_list = top_rated_show.filter("Rank < 2").orderBy("release_year", "Rank")
top_rated_show_list.show(20)
