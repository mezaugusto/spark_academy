import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import org.apache.spark.sql.functions.{ desc, count, sum, bround }
 
object C7Assignment {

  def main(args: Array[String]) {
    val appName : String = "C7 Assignment"
    val attempt = "1"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val spark = SparkSession.builder
      .config(conf = conf)
      .appName(appName)
      .getOrCreate

    def load_jsonl(dataset_bucket_path: String, only_one: Boolean = false): Dataset[Row] = {
      val file_wildcard = if (only_one) { "*00000" } else { "*" }
      spark.read.format("json").load(dataset_bucket_path + s"$file_wildcard.jsonl.gz")
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

    val base_path = "gs://de-training-output-augustomeza/assignment-7"
    val digits = 2
    val orders_ds = load_jsonl("gs://de-training-input/alimazon/200000/client-orders/")
    val stocks_ds = load_jsonl("gs://de-training-input/alimazon/200000/stock-orders/")
    val clients_ds = load_jsonl("gs://de-training-input/alimazon/200000/clients/")

    val orders_by_client = orders_ds.groupBy("client_id").
      agg(
        count("id") as "total_transactions",
        bround(sum("total"), digits) as "total_amount",
        sum("quantity") as "total_products"
      ).
      orderBy(desc("total_amount"), $"total_transactions").
      limit(10)
    
    val best_clients = clients_ds.
      join(orders_by_client, clients_ds.col("id") === orders_by_client.col("client_id")).
      drop("id").
      select("client_id", "name", "gender", "country", "registration_date", "total_transactions", "total_amount", "total_products").
      coalesce(1).
      orderBy(desc("total_amount"), $"total_transactions")
    
    best_clients.explain()

    val invalid_clients = orders_ds.
      join(clients_ds, clients_ds.col("id") === orders_ds.col("client_id"), "leftanti").
      select($"client_id".alias("invalid_client_id"), $"id".alias("order_id"))
    
    val orders_by_product = orders_ds.groupBy("product_id").
      agg(sum("quantity") as "total_sold")

    val stocks_by_product = stocks_ds.groupBy("product_id").
      agg(sum("quantity") as "total_bought")

    val product_stats = stocks_by_product.
      join(orders_by_product, "product_id").
      withColumn("is_valid", $"total_bought" >= $"total_sold")

    writeDataFrame(best_clients, s"$base_path/best-clients")
    writeDataFrame(invalid_clients, s"$base_path/invalid-clients")
    writeDataFrame(product_stats, s"$base_path/product_stats")
  }
}