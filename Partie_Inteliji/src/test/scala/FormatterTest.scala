import org.apache.spark.sql.{DataFrame, SparkSession}
import sda.traitement.ServiceVente
import org.scalatest.funsuite.AnyFunSuite

class FormatterTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("FormatterTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import ServiceVente.DataFrameUtils

  test("verification de la fonction formatter") {

    val inputData = Seq(
      ("100,5|0,19"),
      ("120,546|0,20"),
      ("0|0,15")
    ).toDF("HTT_TVA")

    // Données attendues après la transformation avec formatter()
    val expectedData = Seq(
      ("100,5|0,19", "100,5", "0,19"),
      ("120,546|0,20", "120,546", "0,20"),
      ("0|0,15", "0", "0,15")
    ).toDF("HTT_TVA", "HTT", "TVA")

    // Appliquer la fonction formatter
    val resultData = inputData.formatter()

    // Vérifier que les colonnes sont dans le bon ordre
    val resultDataOrdered = resultData.select("HTT_TVA", "HTT", "TVA")
    val expectedDataOrdered = expectedData.select("HTT_TVA", "HTT", "TVA")

    // Trier les DataFrames pour éviter les problèmes liés à l'ordre des lignes
    val resultSorted = resultDataOrdered.orderBy("HTT_TVA")
    val expectedSorted = expectedDataOrdered.orderBy("HTT_TVA")

    // Vérification finale
    assert(resultSorted.collect() === expectedSorted.collect())
  }
}
