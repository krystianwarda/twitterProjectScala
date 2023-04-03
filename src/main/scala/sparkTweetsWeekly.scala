// Import required packages
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import java.util.Date
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._
import credentials._

// Define the object containing the main function

object tweetsHourlyClean {
  def main(args: Array[String]): Unit = {

    val tempDatabaseName = databaseName
    val tempTableName = tableName
    val tempDatabaseUserName = databaseUserName
    val tempDatabaseUserPassword = databaseUserPassword

    // Create a SparkSession object
    val spark = SparkSession.builder().master("local[*]")
      .appName("SparkByExamples.com")
      .config("spark.driver.memory", "50g")
      .config("spark.memory.offHeap.enabled", true)
      .config("spark.memory.offHeap.size", "50g")
      .getOrCreate()

    // Read data from a MySQL table into a DataFrame
    val df_raw = spark.read
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/" + tempDatabaseName)
      .option("dbtable", tableName)
      .option("user", databaseUserName)
      .option("password", databaseUserPassword)
      .load()

    // Define time span
    val format = new SimpleDateFormat("dd/MM/yyyy") 
    var beginTime = "01/01/2022"
    var endTime = "15/01/2023"  

    // Define user-defined functions to convert date strings to instant objects and vice versa
    val toInstant = udf { dateString: String =>
      val formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy")
      Instant.from(formatter.parse(dateString)).atOffset(ZoneOffset.UTC).toInstant
    }

    val toString = udf { instant: Instant =>
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      LocalDateTime.ofInstant(instant, ZoneOffset.UTC).format(formatter)
    }

    // Convert the "created_at" column to instant objects, extract the "date" column, and filter by a date range
    var df = df_raw.withColumn("date", toInstant(col("created_at")))
      .withColumn("date", from_utc_timestamp(toString(col("date")), "UTC"))
      .withColumn("beginTime", lit(beginTime))
      .withColumn("beginTime",to_date(col("beginTime"), "d/M/y"))
      .withColumn("endTime", lit(endTime))
      .withColumn("endTime",to_date(col("endTime"), "d/M/y"))
      .select("beginTime", "endTime", "created_at", "date", "user_screen_name", "text", "entities_hashtags_list")
      .filter(col("date") > col("beginTime"))
      .filter(col("date") < col("endTime"))

    // // Get first 5 days with the most number of tweets
    // // Aggregate the number of records by date and sort in descending order
    // var df_temp = df.select("date").withColumn("date",date_format(col("date"), "yyyy-MM-dd"))
    //   .groupBy("date").agg(expr("count(date)").alias("number_of_records"))
    //   .groupBy("date")
    //   .agg(col("date").alias("date_new"), sum("number_of_records").alias("number_of_records"))
    //   .drop("date").orderBy(desc("number_of_records"))
    // println(df_temp.show(5))


    // // Get first 5 accounts with the highest number of tweets
    // var df_temp = df.select("user_screen_name")
    //   .groupBy("user_screen_name").agg(expr("count(user_screen_name)").alias("number_of_records"))
    //   .orderBy(desc("number_of_records")).limit(160)
    // println(df_temp.show(5))

    // Get first 5 common hashtags used in tweets
    var df_temp = df.select("entities_hashtags_list")
      .filter(col("entities_hashtags_list") =!= "")
      .withColumn("entities_hashtags_list", explode(split(col("entities_hashtags_list"), "\\,")))
      .groupBy("entities_hashtags_list")
      .count().orderBy(desc("count")).limit(160)
      println(df_temp.show(5))
    





  }
}