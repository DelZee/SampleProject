# From the above files, solve the below given questions:
#
# 1. When you get a new data set what are the steps you usually follow to get the details of it. Like how you go about knowing the data and its analysis.

# The type of format the dataset is in.
# One of the first steps is to check for completeness of the dataset. An incomplete dataset can severely affect the analysis that is being conducted on the dataset.
# Learning about the schema of the dataset, the type of datatypes assigned to each column and if they are rightly assigned to each column, whether the empty columns are populated with null values
# The Number of rowsn and columns. Getting the first 10 rows from the table to understand quickly what is in the dataset.

# 2. Write Spark SQL to get
# a. product wise sales
# b. which product has produced highest sales

from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("First Pyspark Session") \
        .master("local[*]") \
        .getOrCreate()

#creating a dataframe for the csv file
products = spark.read.option("Header", True).csv("C:/Users/delis/Documents/Products/products.csv")
products.show()
products.printSchema()
print(type(products))

sales = spark.read.option("Header", True).csv("C:/Users/delis/Documents/Products/sales.csv")
sales.show()

sellers = spark.read.option("Header", True).csv("C:/Users/delis/Documents/Products/sellers.csv")
sellers.show()

#joining two dataframes

products.join(sales,
              products.product_id == sales.product_id,
              "inner").drop(products.product_id).show()










#Creating SQL Tables
spark.read \
        .option("Header", True) \
        .csv("C:/Users/delis/Documents/Products/products.csv") \
        .createOrReplaceTempView("Products")
spark.read \
        .option("Header", True) \
        .csv("C:/Users/delis/Documents/Products/sales.csv") \
        .createOrReplaceTempView("Sales")
spark.read \
        .option("Header", True) \
        .csv("C:/Users/delis/Documents/Products/sellers.csv") \
        .createOrReplaceTempView("Sellers")


#writing queries
spark.sql("""SELECT * FROM Products""") \
         .show(5)
spark.sql("""SELECT * FROM Sales""") \
         .show(5)
spark.sql("""SELECT * FROM Sellers""") \
         .show(5)

# 2. Write Spark SQL to get
# a. product wise sales
# b. which product has produced highest sales

spark.sql("""SELECT  p.product_id
                    ,p.product_name 
                    ,SUM(p.price * s.num_pieces_sold) as total_sales   
             FROM Products as p 
             JOIN Sales as s on p.product_id = s.product_id
             group by p.product_id, p.product_name
             order by total_sales DESC
             """) \
         .show()

spark.sql("""SELECT  p.product_id
                    ,p.product_name 
                    ,SUM(p.price * s.num_pieces_sold) as total_sales   
             FROM Products as p 
             JOIN Sales as s on p.product_id = s.product_id
             group by p.product_id, p.product_name
             order by total_sales DESC
             LIMIT (1)
             """) \
         .show()

