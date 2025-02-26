name := "Projet_Scala_v2"
version := "1.0"
scalaVersion := "2.12.15"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
libraryDependencies += "com.beust" % "jcommander" % "1.48"
libraryDependencies += "com.databricks" % "spark-xml_2.12" % "0.15.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.16" % Test