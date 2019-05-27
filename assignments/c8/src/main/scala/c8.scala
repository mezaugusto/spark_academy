import java.util.Base64.getDecoder
import org.apache.spark.sql.catalyst.expressions.{ GenericRowWithSchema }
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.functions.{ udf }
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType, ArrayType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

 
object C8Assignment {

  def main(args: Array[String]) {
    val appName : String = "C8 Assignment"
    val attempt = "0"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val spark = SparkSession.builder
      .config(conf = conf)
      .appName(appName)
      .getOrCreate

    val address = StructType(
      List(
        StructField("streetAddress", StringType, false),
        StructField("city", StringType, false),
        StructField("state", StringType, false),
        StructField("postalCode", LongType, false)
      )
    )

    val image = StructType(
      List(
        StructField("data", StringType, false)
      )
    )

    val phoneNumbers = ArrayType(
      StructType(
        List(
          StructField("type", StringType, false),
          StructField("number", StringType, false)
        )
      )
    )

    val schema = StructType(
      List(
        StructField("firstName", StringType, false),
        StructField("lastName", StringType, false),
        StructField("isAlive", BooleanType, false),
        StructField("age", LongType, false),
        StructField("address", address, false),
        StructField("phoneNumbers", phoneNumbers, false),
        StructField("image", image, false)
      )
    )

    val profiles = spark.read.schema(schema).json("gs://de-training-input/profiles/")

    val metadata = StructType(
      List(
        StructField("magic", StringType, false),
        StructField("width", LongType, false),
        StructField("height", LongType, false),
        StructField("max_value", LongType, false)
      )
    )

    val newImage = StructType(
      List(
        StructField("data", StringType, false),
        StructField("metadata", metadata, false)
      )
    )

    val newSchema = StructType(
      List(
        StructField("firstName", StringType, false),
        StructField("lastName", StringType, false),
        StructField("isAlive", BooleanType, false),
        StructField("age", LongType, false),
        StructField("address", address, false),
        StructField("phoneNumbers", phoneNumbers, false),
        StructField("image", newImage, false)
      )
    )

    def DecodeBase64(buffer: String): String = {
      val ascii_values = getDecoder.
        decode(buffer).
        span(decimal => decimal > -1 && decimal < 128).
        _1
      new String(ascii_values)
    }

    def ExtractImageMetadata: (Row => Row) = row => {
      val data = row.getAs[String]("data")
      val tokens = DecodeBase64(data)
        .split("\n")
        .toSeq
        .flatMap(line => {
            line.split("#")(0).split("\\s")
        }).
        filter(_ != "")
      Row(data, Row(tokens(0), tokens(1).toLong, tokens(2).toLong, tokens(3).toLong))
    }

    val extractUDF = udf(
      (r: Row) => ExtractImageMetadata(r),
      newImage
    )

    profiles.
      withColumn("_image", extractUDF($"image")).
      drop("image").
      select($"firstName", $"lastName", $"isAlive", $"age", $"address", $"phoneNumbers", $"_image" as "image").
      write.
      json(s"gs://de-training-output-augustomeza/assignment_8/$attempt")
  }
}