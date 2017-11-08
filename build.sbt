name := "spark-app"

version := "0.0.1"

scalaVersion := "2.11.11"

assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"

val sparkVersion = "2.2.0"

val sparkAvroVersion = "4.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.databricks" %% "spark-avro" % sparkAvroVersion
)
