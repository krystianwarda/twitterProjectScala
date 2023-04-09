// Import required packages
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import credentials._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.DataFrame

// Define the object containing the main function
object userData {
  def main(args: Array[String]): Unit = {

    val tempDatabaseName = databaseName
    val tempTableName = tableName
    val tempDatabaseUserName = databaseUserName
    val tempDatabaseUserPassword = databaseUserPassword

    def exportToCsv(df: DataFrame, fileName: String): Unit = {
      df.coalesce(1) // To ensure output as single file
        .write
        .mode("overwrite")
        .option("header", "true") // write column headers
        .option("delimiter", ",") // set delimiter
        .csv("data/reports/" + fileName)
    }


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

        // Define time span and user
    val format = new SimpleDateFormat("dd/MM/yyyy") 
    var beginTime = "01/01/2022"
    var endTime = "31/01/2023"
    var userTemp = "PIERPAO67864611"

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
    var df = df_raw.filter(col("user_screen_name") === userTemp)
      .withColumn("date", toInstant(col("created_at")))
      .withColumn("date", from_utc_timestamp(toString(col("date")), "UTC"))
      .withColumn("beginTime", lit(beginTime))
      .withColumn("beginTime",to_date(col("beginTime"), "d/M/y"))
      .withColumn("endTime", lit(endTime))
      .withColumn("endTime",to_date(col("endTime"), "d/M/y"))
      .select("beginTime", "endTime", "created_at", "date", "user_screen_name", "text", "entities_hashtags_list", "trackDate", "favorite_count", "retweet_count", "source", "id")
      .filter(col("date") > col("beginTime"))
      .filter(col("date") < col("endTime"))

    df.persist(StorageLevel.MEMORY_AND_DISK)

    // Get information about user activity broken down by posting hours
    var df_temp = df.select(col("date"))
     .withColumn("hour", hour(col("date")))
     .select(col("hour"))
     .groupBy("hour").count().orderBy(desc("count"))
    exportToCsv(df_temp, "hoursActivity.csv")

    println(df_temp.show(5))

    // Get the information about a devices, which account is using to post the tweets
    df_temp = df.select(col("source"))
      .groupBy("source").count().orderBy(desc("count"))

    exportToCsv(df_temp, "deviceActivity.csv")
    println(df_temp.show(5))

    // Get a list of hashtags used by the account broken down by days
    df_temp = df.select("date", "entities_hashtags_list")
      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
      .filter(col("entities_hashtags_list") =!= "")
      .withColumn("entities_hashtags_list", explode(split(col("entities_hashtags_list"), "\\,")))
      .groupBy("entities_hashtags_list", "date").count()
      .filter(col("count") >= 10)
      .groupBy("date")
      .agg(collect_list(struct(col("entities_hashtags_list"), col("count"))).as("dict"))
      .select(col("date"), map_from_entries(col("dict")).as("entities_hashtags_count_dict"))
      .withColumn("entities_hashtags_count_dict", map_from_arrays(sort_array(map_keys(col("entities_hashtags_count_dict")), false), sort_array(map_values(col("entities_hashtags_count_dict")), false)))
      .select(col("date"), col("entities_hashtags_count_dict").cast("string"))

    exportToCsv(df_temp, "hashtagsActivity.csv")
    println(df_temp.show(5))
    val randomRow = df_temp.orderBy(rand()).limit(1).first()

    println("Randomly selected row:")
    randomRow.toSeq.foreach(println)

    df.unpersist()

  }
}


// | PIERPAO67864611|            54998|
// |     papay3guyyy|            49670|
// |         Mone_fb|            44240|
// |     Freedom5019|            40561|
// |       FBeringar|            36987|

//  |-- id: long (nullable = true)
//  |-- created_at: string (nullable = true)
//  |-- source: string (nullable = true)
//  |-- text: string (nullable = true)
//  |-- is_quote_status: boolean (nullable = true)
//  |-- retweet_count: long (nullable = true)
//  |-- favorite_count: long (nullable = true)
//  |-- user_screen_name: string (nullable = true)
//  |-- entities_screen_name_list: string (nullable = true)
//  |-- entities_user_id_list: string (nullable = true)
//  |-- entities_hashtags_list: string (nullable = true)
//  |-- trackDate: long (nullable = true)
//  |-- quoted_status_id: long (nullable = true)
//  |-- quoted_status_created_at: string (nullable = true)
//  |-- quoted_status_text: string (nullable = true)
//  |-- quoted_status_user_id: long (nullable = true)
//  |-- quoted_status_user_screen_name: string (nullable = true)
//  |-- quoted_status_user_created_at: string (nullable = true)
//  |-- retweeted_status_id: long (nullable = true)
//  |-- retweeted_status_created_at: string (nullable = true)
//  |-- retweeted_status_text: string (nullable = true)
//  |-- retweeted_status_user_id: string (nullable = true)
//  |-- retweeted_status_user_screen_name: string (nullable = true)
//  |-- retweeted_status_user_created_at: string (nullable = true)
//  |-- in_reply_to_status_id: long (nullable = true)
//  |-- in_reply_to_user_id: long (nullable = true)
//  |-- in_reply_to_screen_name: string (nullable = true)