import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import org.apache.spark.sql.functions.{ desc, date_format }
import org.apache.spark.sql.types.{ IntegerType, StructField, StructType }
import java.time.{ LocalDateTime, Duration }
 
object C5Assignment {

  def main(args: Array[String]) {
    val appName : String = "C5 Assignment"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val spark = SparkSession.builder
      .config(conf = conf)
      .appName(appName)
      .getOrCreate

    val base_path = "gs://de-training-output-augustomeza/assignment-5"
    val attempt = "2"

    def load_jsonl(dataset_bucket_path: String): Dataset[Row] = {
      spark.read.format("json").load(dataset_bucket_path + "*.jsonl.gz")
    }

    def getPartitionsDF(df: Dataset[Row]):Dataset[Row] = {
      val schema = new StructType().
        add(StructField("partition_size", IntegerType, /* nullable: */ false))
        
      val partitions = df.rdd.mapPartitionsWithIndex(
        (index, partition) => List(Row(partition.size)).toIterator)

      return spark.createDataFrame(partitions, schema).coalesce(1).orderBy(desc("partition_size"))
    }

    def printDuration(start: LocalDateTime, end: LocalDateTime) = {
      println("Duration:" + Duration.between(start, end).toMillis())
    }

    def writeDataFrame(dataframe: Dataset[Row], path: String, partitions: Int = 0) = {
      val df = if (partitions>0) {
        dataframe.repartition(partitions)
      } else {
        dataframe
      }
      df.
        write.
        format("com.databricks.spark.csv").
        option("header", true).
        option("delimiter", ",").
        save(s"$path/$attempt")
    }

    val orders_ds = load_jsonl("gs://de-training-input/alimazon/200000/client-orders/")

    writeDataFrame(getPartitionsDF(orders_ds), s"$base_path/default-partitions")

    val byID = orders_ds.repartitionByRange($"id")
    writeDataFrame(getPartitionsDF(byID), s"$base_path/client_id-partitions")

    val by15 = orders_ds.repartition(15)
    writeDataFrame(getPartitionsDF(by15), s"$base_path/fifteen-partitions")

    val by15_client_id = orders_ds.repartitionByRange(15, $"client_id")
    writeDataFrame(getPartitionsDF(by15_client_id), s"$base_path/fifteen-client_id-partitions")

    val by15_product_id_month = orders_ds.
      withColumn("month", date_format($"timestamp", "MMMMM")).
      repartitionByRange(15, $"product_id", $"month")
    writeDataFrame(getPartitionsDF(by15_product_id_month), s"$base_path/fifteen-product_id-month-partitions")

    val num_partitions = Seq(10, 150, 800)
    val stocks_ds = load_jsonl("gs://de-training-input/alimazon/200000/stock-orders/")
    num_partitions.map(partition => {
      val start = LocalDateTime.now()
      writeDataFrame(stocks_ds, s"$base_path/$partition-files", partition)
      println(s"Stocks with $partition partition(s)")
      printDuration(start, LocalDateTime.now())
    })
  }
}