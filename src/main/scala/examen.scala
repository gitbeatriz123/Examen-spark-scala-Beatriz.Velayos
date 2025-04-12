import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object examen {

  /**
   * Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   *
   * @param estudiantes DataFrame con columnas (nombre: String, edad: Int, calificacion: Double)
   * @return DataFrame con los nombres de los estudiantes con calificación > 8, ordenados descendentemente
   */
  def ejercicio1(estudiantes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    estudiantes
      .filter(col("calificacion") > 8)
      .select("nombre", "calificacion")
      .orderBy(col("calificacion").desc)
  }

  /**
   * Ejercicio 2: UDF para determinar si un número es par o impar
   */
  def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val esParUDF = udf((n: Int) => if (n % 2 == 0) "par" else "impar")
    numeros.withColumn("par_o_impar", esParUDF(col("numero")))
  }
  /**
   * Ejercicio 3: Joins y agregaciones
   *
   * @param estudiantes DataFrame con columnas (id: Int, nombre: String)
   * @param calificaciones DataFrame con columnas (id_estudiante: Int, asignatura: String, calificacion: Double)
   * @return DataFrame con columnas (id, nombre, promedio_calificacion)
   */
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
      .groupBy("id", "nombre")
      .agg(avg("calificacion").alias("promedio_calificacion"))
  }
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.SparkSession

  def ejercicio4(palabras: List[String])(spark: SparkSession): RDD[(String, Int)] = {
    val rdd = spark.sparkContext.parallelize(palabras)
    val conteo = rdd.map(palabra => (palabra, 1)).reduceByKey(_ + _)
    conteo
  }
  def ejercicio5(ventas: DataFrame)(implicit spark: SparkSession): DataFrame = {
    ventas
      .withColumn("ingreso", col("cantidad") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(sum("ingreso").alias("ingreso_total"))
      .orderBy("id_producto")
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ejercicio5App")
      .master("local[*]")
      .getOrCreate()

    val ventas = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/scala/examenbeatrizvelayos/ventas.csv")

    val resultado = ejercicio5(ventas)(spark)
    resultado.show()

    spark.stop()
  }



}
