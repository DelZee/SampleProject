from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read from MySQL").config("spark.sql.warehouse.dir","/user/hive/warehouse").enableHiveSupport().getOrCreate()

jdbcHostname = "savvients-classroom.cefqqlyrxn3k.us-west-2.rds.amazonaws.com"
jdbcPort = 3306
jdbcDatabase = "practical_exercise"
jdbcUsername = "sav_proj"
jdbcPassword = "authenticate"
jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "com.mysql.jdbc.Driver"
}

spark.sql("SHOW DATABASES")
spark.sql("USE DEL")
df_user = spark.read.jdbc(url=jdbcUrl, table="user", properties=connectionProperties)
df_user.show()
df_user.write.mode("overwrite").saveAsTable("DEL.usertable")
spark.sql("select * from usertable limit(5) ").show()

df_actlog = spark.read.jdbc(url=jdbcUrl, table="activitylog", properties=connectionProperties)
df_actlog.show()
df_actlog.write.mode("overwrite").saveAsTable("DEL.activitytable")
spark.sql("select * from activitytable limit(5)").show()

#periodic csv dumps in this table.
spark.sql("CREATE  TABLE IF NOT EXISTS DEL.project (user_id int,file_name string,time_stmp bigint) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','STORED AS TEXTFILE tblproperties('skip.header.line.count'='1')")


## USER_TOTAL TABLE
##creating the user total table
spark.sql("""CREATE TABLE IF NOT EXISTS  DEL.user_total
          (time_ran timestamp
          ,total_users int
          ,users_added int)
          ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
          STORED AS TEXTFILE""")

##creating the user report table
spark.sql("""CREATE TABLE IF NOT EXISTS DEL.user_report
          (user_id int
          ,total_updates int
          ,total_inserts int
          ,total_deletes int
          ,last_activity_type string
          ,is_active boolean
          ,upload_count int)
          ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
          STORED AS TEXTFILE""")


spark.sql("""
    INSERT INTO user_total
    SELECT
        t1.time_ran,
        t1.total_users,
        t1.total_users - COALESCE(t2.total_users, 0) AS users_added
    FROM (
            SELECT CURRENT_TIMESTAMP() AS time_ran, COUNT(*) AS total_users
            FROM usertable
         ) t1
    LEFT JOIN
    (
            SELECT time_ran, total_users
            FROM user_total
    )
        t2 ON t1.time_ran > t2.time_ran
            ORDER BY t1.time_ran
""")

spark.sql("select * from user_total").show()


#USER_REPORT TABLE
spark.sql("""
      INSERT OVERWRITE TABLE user_report
          SELECT
             usertable.id AS user_id,
             COALESCE(SUM(CASE WHEN activitytable.type = 'UPDATE' THEN 1 ELSE 0 END)) AS total_updates,
             COALESCE(SUM(CASE WHEN activitytable.type = 'INSERT' THEN 1 ELSE 0 END)) AS total_inserts,
             COALESCE(SUM(CASE WHEN activitytable.type = 'DELETE' THEN 1 ELSE 0 END)) AS total_deletes,
             MAX(activitytable.type) AS last_activity_type,
             CASE WHEN CAST(from_unixtime(MAX(activitytable.timestamp)) AS DATE) >= DATE_SUB(CURRENT_TIMESTAMP() ,2) THEN true ELSE false END AS is_active,
             COALESCE(COUNT(project.user_id)) AS upload_count
         FROM usertable
         LEFT JOIN activitytable ON usertable.id = activitytable.user_id
         LEFT JOIN project ON usertable.id = activitytable.user_id
         GROUP BY usertable.id""")
spark.sql("SELECT * FROM user_report").show()
