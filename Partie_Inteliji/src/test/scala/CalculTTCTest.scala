import org.apache.spark.sql.{DataFrame, SparkSession}
import sda.traitement.ServiceVente
import org.scalatest.funsuite.AnyFunSuite

class CalculTTCTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("CalculTTCTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import ServiceVente.DataFrameUtils

  test("verification de la fonction CalculTTC") {

    // Données d'entrée simulant les valeurs déjà formatées
    val inputDf = Seq(
      ("100,5|0,19", "100,5", "0,19"),
      ("120,546|0,20", "120,546", "0,20"),
      ("0|0,15", "0,0", "0,15")
    ).toDF("HTT_TVA", "HTT", "TVA")

    // Données attendues après calculTTC()
    val expectedDf = Seq(
      ("100,5|0,19", 119.6),
      ("120,546|0,20", 144.66),
      ("0|0,15", 0.0)
    ).toDF("HTT_TVA", "TTC")

    // Appliquer calculTTC
    val resultDf = inputDf.calculTTC()
    resultDf.show() // Debug

    // Trier et comparer les DataFrames
    val resultSorted = resultDf.orderBy("HTT_TVA")
    val expectedSorted = expectedDf.orderBy("HTT_TVA")

    // Vérification finale
    assert(resultSorted.collect() === expectedSorted.collect())
  }
}
