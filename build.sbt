name := "ScalaSparkCassandraDataCopy"

version := "0.7.1"
scalaVersion := "2.10.5"

resolvers += "DataStax Repo" at "https://datastax.artifactoryonline.com/datastax/public-repos/"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.4.0" % "provided"
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.4.0-M2"
libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.10.5" % "provided"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.10.5" % "provided"
libraryDependencies += "org.scala-lang" % "scalap" % "2.10.5" % "provided"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.10.5" % "provided"