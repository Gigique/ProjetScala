package sda.traitement
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._


object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter()= {
      dataFrame
        .withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }

    def calculTTC () : DataFrame ={
      dataFrame
        .withColumn("HTT", regexp_replace(col("HTT"), ",", ".").cast("double"))
        .withColumn("TVA", regexp_replace(col("TVA"), ",", ".").cast("double"))
        .withColumn("TTC", round(col("HTT") + col("HTT") * col("TVA"), 2))
        .drop("HTT", "TVA")

    }

    def extractDateEndContratVille(): DataFrame = {
      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, false)
        .add("Date_End_contrat", StringType, false)

      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)

      dataFrame
        .withColumn("MetaTransaction", from_json(col("MetaData"), schema))
        .withColumn("MetaTransaction", col("MetaTransaction.MetaTransaction").getItem(0))
        .select(
          col("MetaTransaction.Ville").alias("Ville"),
          col("MetaTransaction.Date_End_contrat").alias("Date_End_contrat"),
          col("*"))
        .withColumn("Date_End_contrat", regexp_extract(col("Date_End_contrat"), "(\\d{4}-\\d{2}-\\d{2})", 0))
        .withColumn("Date_End_contrat", to_date(col("Date_End_contrat"), "yyyy-MM-dd"))
        .drop("MetaData", "MetaTransaction")

    }

    def contratStatus(): DataFrame = {
      dataFrame
        .withColumn("contratStatus", datediff(current_date(), col("Date_End_contrat")))
        .withColumn("contratStatus", when(col("contratStatus") > 0, "Expired").otherwise("Active"))
    }


  }

}