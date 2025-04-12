import org.apache.spark.sql.SparkSession
import spUtils.SparkUtils.runSparkSession

object Main {


  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = runSparkSession("Keepkoding")
    val df = spark.read.csv("src/main/scala/examenbeatrizvelayos/ventas.csv")

    df.show(false)


  }















}