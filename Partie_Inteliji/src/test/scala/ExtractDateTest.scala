import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import sda.traitement.ServiceVente

class ExtractDateTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("ExtractDateTest")
    .master("local[*]")
    .getOrCreate()

  import ServiceVente.DataFrameUtils
  import spark.implicits._

  test("verification de la fonction extractDateEndContratVille") {

    val inputData = Seq(
      ("""{"MetaTransaction":[{"Ville":"Paris","Date_End_contrat":"2020-12-23 00:00:00"}]}"""),
      ("""{"MetaTransaction":[{"Ville":"Alger","Date_End_contrat":"2023-12-23 00:00:00"}]}"""),
      ("""{"MetaTransaction":[{"Ville":"Dakar","Date_End_contrat":"2020-12-23 00:00:00"}]}""")
    ).toDF("MetaData")

    // Données attendues après la transformation avec formatter()
    val expectedData = Seq(
      ("Paris", java.sql.Date.valueOf("2020-12-23")),
      ("Alger", java.sql.Date.valueOf("2023-12-23")),
      ("Dakar", java.sql.Date.valueOf("2020-12-23"))
    ).toDF("Ville", "Date_End_contrat")

    // Appliquer la fonction formatter
    val resultData = inputData.extractDateEndContratVille()

    // Vérifier que les colonnes sont dans le bon ordre
    val resultDataOrdered = resultData.select("Ville", "Date_End_contrat")
    val expectedDataOrdered = expectedData.select("Ville", "Date_End_contrat")

    // Trier les DataFrames pour éviter les problèmes liés à l'ordre des lignes
    val resultSorted = resultDataOrdered.orderBy("Ville")
    val expectedSorted = expectedDataOrdered.orderBy("Ville")

    // Vérification finale
    assert(resultSorted.collect() === expectedSorted.collect())
  }
}
