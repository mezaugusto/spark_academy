import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry, IndexedRow }
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.functions.{ col, max, size }
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
 
object C2Assignment {

  def main(args: Array[String]) {
    val appName : String = "C2 Assignment"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val spark = SparkSession.builder
    .config(conf = conf)
    .appName(appName)
    .getOrCreate

    def df_from_csv(filename: String) : Dataset[Row] = {
      spark.read.format("csv").
        option("inferSchema", "true").
        load(filename)
    }

    def createBlockMatrix(entries: RDD[MatrixEntry]): BlockMatrix = {
      val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)
      coordMat.toBlockMatrix().cache()
    }

    def explodeValues(r : Row) = {
      var newValues = new ListBuffer[MatrixEntry]()
      for (i <- 1 to r.size - 1) newValues += MatrixEntry( r.getInt(0), i - 1, r.getDouble(i) )
      newValues
    }

    def matrix_from_df(df: Dataset[Row]): BlockMatrix = {
      val rdd_matrix = df.rdd.flatMap(explodeValues)
      createBlockMatrix(rdd_matrix)
    }

    def matrix_to_indexed_row(m : BlockMatrix): Dataset[Row] = {
      val df = m.
          toIndexedRowMatrix.
          rows.
          map(r => (r.index, r.vector.toArray) ).
          toDF("index", "values").
          orderBy("index")
      val m_size = df.select(max(size(df("values")))).first().getInt(0)
      val cols = Seq(col("index")) ++ (0 until m_size).map(i => col("values")(i).alias(s"value_$i")) 
      df.select(cols: _*)
    }

    val df1 = df_from_csv("gs://de-training-input/matrices/matrix1.txt")
    val df2 = df_from_csv("gs://de-training-input/matrices/matrix2.txt")

    val m1 = matrix_from_df(df1)
    val m2 = matrix_from_df(df2)
    val result = m1.multiply(m2)

    matrix_to_indexed_row(result).
      coalesce(1).
      write.
      format("com.databricks.spark.csv").
      option("header", false).
      option("delimiter", ",").
      save("gs://de-training-output-augustomeza/assignments/c2/output")
  }
}