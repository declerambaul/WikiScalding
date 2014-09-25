import xerial.sbt.Pack.packSettings

packAutoSettings

packSettings

// needed for 'pack' pluging which also creates a bin directory
packMain := Map("wmf" -> "com.twitter.scalding.Tool")

net.virtualvoid.sbt.graph.Plugin.graphSettings

name := "WikiScalding"

resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "MvnRepository" at "http://mvnrepository.com/artifact")

// to use the fasterxml jackson extensions for scala, we have to exclude older dependencies on jackson coming scalding
libraryDependencies ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.4.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.4.2",
  "com.twitter" %% "scalding-core" % "0.12.0rc3" exclude("org.codehaus.jackson", "jackson-core-asl") exclude("org.codehaus.jackson", "jackson-mapper-asl"),
  "com.twitter" %% "scalding-repl" % "0.12.0rc3" exclude("org.codehaus.jackson", "jackson-core-asl") exclude("org.codehaus.jackson", "jackson-mapper-asl"),
  "org.apache.hadoop" % "hadoop-client" % "2.3.0-mr1-cdh5.0.2"  exclude("org.codehaus.jackson", "jackson-core-asl") exclude("org.codehaus.jackson", "jackson-mapper-asl"))
