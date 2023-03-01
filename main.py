# from pyspark.sql import SparkSession

# if __name__ == "__main__":
#
#     spark = SparkSession \
#         .builder \
#         .appName("First Pyspark Session") \
#         .master("local[*]") \
#         .getOrCreate()
#
#     input_file_path = "file:///C:/Users/delis/Downloads/raw_titles.csv"
#
#
# #Creating a dataframe for the two csv files
#     titles_df = spark.read.option("Header", True).csv("C:/Users/delis/Downloads/raw_titles.csv")
#     titles_df.show()
#     titles_df.printSchema()
#     credits_df = spark.read.option("Header", True).csv("C:/Users/delis/Downloads/raw_credits.csv")
#     credits_df.show()
#     credits_df.printSchema()
#
# #Creating SQL Tables
#     spark.read \
#         .option("Header", True) \
#         .csv("C:/Users/delis/Downloads/raw_titles.csv") \
#         .createOrReplaceTempView("Titles")
#
#     spark.read \
#         .option("Header", True) \
#         .csv("C:/Users/delis/Downloads/raw_credits.csv") \
#         .createOrReplaceTempView("Credits")
#
# #Writing a query
#
#     spark.sql("""SELECT id, name, role FROM Credits""") \
#          .show(5)
#
#     spark.sql("""SELECT * From Titles""") \
#         .show(5)
#
# #Analyze the Top rated Movie and the actor that acted in it based on the year
#
# spark.sql("""Select * From
#                           (SELECT
#                                 c.name
#                               , c.role
#                               , t.title
#                               , t.type
#                               , t.release_year
#                               , t.imdb_score
#                               ,ROW_NUMBER() OVER (Partition by t.release_year Order By t.imdb_score asc)  as Rank
#                           From credits as c
#                           join Titles  as t
#                           on c.id = t.id
#                           where c.role = 'ACTOR'
#                           and t.imdb_score is not null
#                           and t.type = 'MOVIE'
#                           order by Rank ASC
#                                           ) as R
# where Rank < 2
# order by R.release_year, Rank""")\
#     .show(50)
#
#
# #Analyze the Top rated Show and the actor that acted in it based on the year
#
# spark.sql("""Select * From
#                           (SELECT
#                                 c.name
#                               , c.role
#                               , t.title
#                               , t.type
#                               , t.release_year
#                               , t.imdb_score
#                               ,ROW_NUMBER() OVER (Partition by t.release_year Order By t.imdb_score asc)  as Rank
#                           From credits as c
#                           join Titles  as t
#                           on c.id = t.id
#                           where c.role = 'ACTOR'
#                           and t.imdb_score is not null
#                           and t.type = 'SHOW'
#                           order by Rank ASC
#                                             ) as R
# where Rank < 2
# order by R.release_year, Rank """)\
#     .show()

