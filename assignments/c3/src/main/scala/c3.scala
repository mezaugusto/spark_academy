import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import org.apache.spark.sql.functions.{ sum, desc, count, date_format }
 
object C3Assignment {

  def main(args: Array[String]) {
    val appName : String = "C3 Assignment"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val spark = SparkSession.builder
    .config(conf = conf)
    .appName(appName)
    .getOrCreate

    def load_jsonl(dataset_bucket_path: String): Dataset[Row] = {
      spark.read.
        format("json").
        load(dataset_bucket_path + "*.jsonl.gz")
    }
    def writeDataFrame(dataframe: Dataset[Row], path: String) = {
      dataframe.write.
      format("com.databricks.spark.csv").
      option("header", true).
      option("delimiter", ",").
      save(path)
    }

    val base_path = "gs://de-training-output-augustomeza/c3/assignment"
    val attempt = "4"
    val orders_ds = load_jsonl("gs://de-training-input/alimazon/200000/client-orders/")

    // TOP 10 BY WEEK
    val grouped_by_weekday_product = orders_ds.
      withColumn("weekday", date_format($"timestamp", "EEEE")).
      groupBy("product_id", "weekday").
      agg(sum("quantity") as "total_orders", count("id") as "count").
      cache()

    val WEEKDAYS = Seq("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    val weekday_path = s"$base_path/top10_week"

    WEEKDAYS.map(weekday => {
      writeDataFrame(grouped_by_weekday_product.
        select("product_id", "weekday", "count").
        orderBy(desc("count")).
        where($"weekday" <=> weekday).
        limit(10)
      , s"$weekday_path/total_items/$attempt/$weekday")
      writeDataFrame(grouped_by_weekday_product.
        select("product_id", "weekday", "total_orders").
        orderBy(desc("total_orders")).
        where($"weekday" <=> weekday).
        limit(10)
      , s"$weekday_path/total_orders/$attempt/$weekday")
    })

    // TOP 10 BY MONTH
    sqlContext.clearCache()

    val grouped_by_client_month = orders_ds.
      withColumn("month", date_format($"timestamp", "MMMMM")).
      groupBy("client_id", "month").
      agg(count("id") as "total_orders", sum("total") as "total_spent").
      cache()
    val MONTHS = Seq("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December")
    val month_path = s"$base_path/top10_month"

    MONTHS.map(month => {
      writeDataFrame(grouped_by_client_month.
        select("client_id", "month", "total_orders").
        orderBy(desc("total_orders")).
        where($"month" <=> month).
        limit(10)
      , s"$month_path/total_orders/$attempt/$month")
      writeDataFrame(grouped_by_client_month.
        select("client_id", "month", "total_spent").
        orderBy(desc("total_spent")).
        where($"month" <=> month).
        limit(10)
      , s"$month_path/total_spent/$attempt/$month")
    })
  }
}