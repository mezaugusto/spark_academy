import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import org.apache.spark.sql.functions.{ sum, desc, count, bround, lit, min, max, avg, date_format }
import java.time.LocalDateTime

 
object C4Assignment {

  def main(args: Array[String]) {
    val appName : String = "C4 Assignment"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val spark = SparkSession.builder
    .config(conf = conf)
    .appName(appName)
    .getOrCreate

    val base_path = "gs://de-training-output-augustomeza"
    val attempt = "2"
    val discount = 0.9
    val digits = 2

    def load_jsonl(dataset_bucket_path: String): Dataset[Row] = {
      spark.
        read.
        format("json").
        load(dataset_bucket_path + "*.jsonl.gz")
    }

    def writeDataFrame(dataframe: Dataset[Row], path: String, repartition: Boolean = true) = {
      dataframe.
        write.
        format("com.databricks.spark.csv").
        option("header", true).
        option("delimiter", ",").
        save(s"$path-$attempt")
    }

    val orders_ds = load_jsonl("gs://de-training-input/alimazon/200000/client-orders/")
    val min_date = LocalDateTime.now().minusMonths(6).toString;

    // Best Selling Hours
    val best_selling_hours = orders_ds.
      withColumn("hour", date_format($"timestamp", "HH")).
      groupBy("hour").
      agg(
          bround(avg("total"), digits) as "average_spending",
          min("total") as "min_spending",
          max("total") as "max_spending"
      ).
      orderBy("average_spending")
    writeDataFrame(best_selling_hours, s"$base_path/assignment-4-best_selling_hours")

    // Monthly Discount
    val monthly_discount = orders_ds.
      filter($"timestamp" >= lit(min_date)).
      groupBy("product_id").
      agg(
          sum("quantity") as "total_items_sold",
          sum("total") as "total_revenue",
          count("id") as "total_registered_sales"
      ).
      orderBy(desc("total_items_sold")).
      limit(10).
      withColumn("unit_product_price", bround($"total_revenue" / $"total_items_sold", digits)).
      withColumn("new_unit_product_price", bround($"unit_product_price" * discount, digits)).
      drop("total_revenue").
      orderBy("total_items_sold")
    writeDataFrame(monthly_discount, s"$base_path/assignment-4-monthly_discount")
    
    // Clients Orders Dist
    val clients_orders_dist = orders_ds.
      groupBy("client_id").
      agg(
          count("id") as "purchases_count"
      ).
      groupBy("purchases_count").
      agg(
          count("client_id") as "clients_count"
      ).
      orderBy("clients_count")
    writeDataFrame(clients_orders_dist, s"$base_path/assignment-4-client_orders_dist")
  }
}