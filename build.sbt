name := "SparkETL"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.7"

/*libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "1.6.3_0.9.0" % "test",
  "org.apache.spark" %% "spark-core" % "1.6.3" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.3" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.6.3" % "provided",
  "com.databricks" % "spark-csv_2.11" % "1.2.0",
  "org.typelevel" %% "cats-core" % "0.4.1"
)*/

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "1.6.3_0.9.0" % "test",
  "org.apache.spark" %% "spark-core" % "1.6.3",
  "org.apache.spark" %% "spark-sql" % "1.6.3",
  "org.apache.spark" %% "spark-hive" % "1.6.3",
  "com.databricks" % "spark-csv_2.11" % "1.2.0",
  "org.typelevel" %% "cats-core" % "0.4.1"
)

libraryDependencies := libraryDependencies.value
  .map(_.exclude("com.sun.jdmk", "jmxtools")
    .exclude("com.sun.jmx", "jmxri").
    exclude("org.eclipse.jetty.orbit", "javax.transaction").
    exclude("org.eclipse.jetty.orbit", "javax.mail").
    exclude("org.eclipse.jetty.orbit", "javax.activation").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("com.esotericsoftware.minlog", "minlog")
  )

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

parallelExecution in Test := false

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x => MergeStrategy.first
}
