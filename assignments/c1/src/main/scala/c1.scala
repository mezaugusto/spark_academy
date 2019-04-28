import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import org.apache.spark.sql.functions.{ sum, desc }
 
object C1Assignment {

  def main(args: Array[String]) {
    val appName : String = "C1 Assignment"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder
    .config(conf = conf)
    .appName(appName)
    .getOrCreate

    /** Load all JSONLines files from directory
    */
    def load_jsonl(dataset_bucket_path: String): Dataset[Row] = {
      spark.read.format("json").load(dataset_bucket_path + "*.jsonl.gz")
    }

    /** Sort by product id while aggregating quantity
    */
    def sort_by_product(dataframe: Dataset[Row] ): Dataset[Row] = {
      dataframe.
        groupBy("product_id").
        agg(sum("quantity").
        alias("orders_count")).
        orderBy(desc("orders_count"))
    }

    val orders_df = load_jsonl("gs://de-training-input/alimazon/50000/client-orders/")
    val orders_by_product_sorted = sort_by_product(orders_df)
    orders_by_product_sorted.
      coalesce(1).
      write.
      json("gs://de-training-output-augustomeza/assignments/c1/output.json")
  }
}