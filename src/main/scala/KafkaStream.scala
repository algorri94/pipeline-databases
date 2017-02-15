import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
  * A simple app that pulls a stream from Kafka, parses the data with Spark and inserts it into MemSQL
  */
object KafkaStream {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaStream").set("spark.memsql.defaultDatabase", "test")
    val streamingContext = new StreamingContext(conf, Seconds(1))
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sqlContext = spark.sqlContext

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9093,localhost:9094",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val schema = StructType(Seq(
      StructField("_id", StringType, false),
      StructField("_rev", StringType, false),
      StructField("dropoff_latitude", StringType, false),
      StructField("dropoff_longitude", StringType, false),
      StructField("hack_license", StringType, false),
      StructField("medallion", StringType, false),
      StructField("passenger_count", IntegerType, false),
      StructField("pickup_latitude", StringType, false),
      StructField("pickup_longitude", StringType, false),
      StructField("trip_distance", StringType, false),
      StructField("trip_time_in_secs", IntegerType, false),
      StructField("vendor_id", StringType, false)
    ))
    stream.map(record => record.value.split(",")).foreachRDD(rdd => {
        //Map each RDD to a DF with the desired Schema so it can be inserted into the Database
        val sql = sqlContext.createDataFrame(rdd.map{
          rdd => Row.fromSeq(rdd.zip(schema.toSeq).map{
            case (value, struct) => convertTypes(value, struct)
          })
        }, schema)
        //Insert the DF into the database
        sql.write.format("com.memsql.spark.connector").mode("error").save("test.taxi")
      }
    )

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def convertTypes(value: String, struct: StructField): Any = struct.dataType match {
    case BinaryType => value.toCharArray().map(ch => ch.toByte)
    case ByteType => value.toByte
    case BooleanType => value.toBoolean
    case DoubleType => value.toDouble
    case FloatType => value.toFloat
    case ShortType => value.toShort
    case DateType => value
    case IntegerType => value.toInt
    case LongType => value.toLong
    case _ => value
  }
}