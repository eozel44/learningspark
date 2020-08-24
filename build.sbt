name := "learningspark"

//version of package
version := "0.1"

//version of Scala
scalaVersion := "2.12.10"

// spark library dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql"  % "3.0.0"
)
