import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import sda.traitement.ServiceVente

class ContratStatusTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("ContratStatusTest")
    .master("local[*]")
    .getOrCreate()

  import ServiceVente.DataFrameUtils
  import spark.implicits._

  test("verification de la fonction ContratStatus") {

    val inputData = Seq(
      ("Paris", java.sql.Date.valueOf("2020-12-23")),
      ("Alger", java.sql.Date.valueOf("2023-12-23")),
      ("Dakar", java.sql.Date.valueOf("2020-12-23"))
    ).toDF("Ville", "Date_End_contrat")

    // Données attendues après la transformation avec formatter()
    val expectedData = Seq(
      ("Paris", java.sql.Date.valueOf("2020-12-23"), "Expired"),
      ("Alger", java.sql.Date.valueOf("2023-12-23"), "Expired"),
      ("Dakar", java.sql.Date.valueOf("2020-12-23"), "Expired")
    ).toDF("Ville", "Date_End_contrat", "contratStatus")

    // Appliquer la fonction formatter
    val resultData = inputData.contratStatus()

    // Vérifier que les colonnes sont dans le bon ordre
    val resultDataOrdered = resultData.select("Ville", "Date_End_contrat", "contratStatus")
    val expectedDataOrdered = expectedData.select("Ville", "Date_End_contrat", "contratStatus")

    // Trier les DataFrames pour éviter les problèmes liés à l'ordre des lignes
    val resultSorted = resultDataOrdered.orderBy("Ville")
    val expectedSorted = expectedDataOrdered.orderBy("Ville")

    // Vérification finale
    assert(resultSorted.collect() === expectedSorted.collect())
  }
}
