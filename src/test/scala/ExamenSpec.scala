import examen.{ejercicio4, ejercicio5}
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{Args, BeforeAndAfterAll, ConfigMap, Filter, FunSuite, Status, Suite, TestData}
import org.apache.spark.sql.functions._

import scala.collection.{GenIterable, immutable}

class ExamenSpec extends FunSuite with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("ExamenTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("ejercicio1 debe filtrar y ordenar correctamente") {

    val sparkLocal = spark // Crear una referencia estable
    import sparkLocal.implicits._

    // Datos iniciales
    val estudiantes = Seq(
      ("Ana", 20, 9.5),
      ("Luis", 22, 7.8),
      ("Juan", 21, 8.9),
      ("María", 23, 6.5)
    ).toDF("nombre", "edad", "calificacion")

    // Filtrar estudiantes con calificaciones mayores a 8 y ordenar por calificación
    val resultado = estudiantes
      .filter($"calificacion" > 8)
      .orderBy($"calificacion".desc)

    // Mostrar los resultados del DataFrame en consola
    resultado.show()

    // Comprobaciones en el test para validar el comportamiento
    val resultadoEsperado = Seq(
      ("Ana", 20, 9.5),
      ("Juan", 21, 8.9)
    ).toDF("nombre", "edad", "calificacion")

    assert(resultado.collect() sameElements resultadoEsperado.collect())
  }



  test("ejercicio2 debe clasificar correctamente números como par o impar") {
    val sparkLocal = spark
    import sparkLocal.implicits._

    // Crear DataFrame de prueba
    val dfNumeros = Seq(1, 2, 3, 4).toDF("numero")

    // Llamar a la función
    val resultado = examen.ejercicio2(dfNumeros)(sparkLocal)

    // Mostrar resultados por consola
    resultado.show()

    // Validación esperada
    val esperado = Seq(
      (1, "impar"),
      (2, "par"),
      (3, "impar"),
      (4, "par")
    ).toDF("numero", "par_o_impar")

    assert(resultado.collect().sameElements(esperado.collect()))
  }
  test("ejercicio3 debe calcular el promedio de calificaciones por estudiante") {
    val sparkLocal = spark
    import sparkLocal.implicits._

    // DataFrame de estudiantes
    val estudiantes = Seq(
      (1, "Ana"),
      (2, "Luis"),
      (3, "Marta")
    ).toDF("id", "nombre")

    // DataFrame de calificaciones
    val calificaciones = Seq(
      (1, "Matemáticas", 8.5),
      (1, "Historia", 9.0),
      (2, "Matemáticas", 6.0),
      (2, "Historia", 7.0),
      (3, "Matemáticas", 10.0)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    val resultado = examen.ejercicio3(estudiantes, calificaciones)

    resultado.show()

    val esperado = Seq(
      (1, "Ana", 8.75),
      (2, "Luis", 6.5),
      (3, "Marta", 10.0)
    ).toDF("id", "nombre", "promedio_calificacion")

    assert(resultado.collect().sameElements(esperado.collect()))
  }
   test("ejercicio4 debe contar las ocurrencias de cada palabra") {
     val palabras = List("scala", "spark", "scala", "big", "data", "spark", "spark")
     val resultadoRDD = ejercicio4(palabras)(spark)

     val resultado = resultadoRDD.collect().sortBy(_._1)

     // Mostrar el resultado por consola
     println("Resultado del ejercicio 4:")
     resultado.foreach(println)

     val esperado = Array(
       ("big", 1),
       ("data", 1),
       ("scala", 2),
       ("spark", 3)
     ).sortBy(_._1)

     assert(resultado.sameElements(esperado))
   }

  test("ejercicio5 calcula correctamente el ingreso_total por id_producto") {
    // Cargar el CSV
    val ventas: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/scala/examenbeatrizvelayos/ventas.csv")

    // Ejecutar la función
    val resultado: DataFrame = ejercicio5(ventas)

    // Mostrar resultados por consola
    resultado.show()

    // Assert simple para verificar que se calculó la columna ingreso_total
    assert(resultado.columns.contains("ingreso_total"))
  }














}