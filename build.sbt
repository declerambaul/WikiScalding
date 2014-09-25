import AssemblyKeys._ // put this at the top of the file,leave the next line blank
import xerial.sbt.Pack.packSettings

packAutoSettings

packSettings

packMain := Map("wmf" -> "com.twitter.scalding.Tool")

net.virtualvoid.sbt.graph.Plugin.graphSettings

//mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
//  {
//    case PathList("com", "esotericsoftware", "minlog", _) => MergeStrategy.first
//    case PathList("com", "twitter", "scalding",  xs @ _*) => MergeStrategy.first
//    case PathList("javax",  xs @ _*) => MergeStrategy.first
//    case PathList("org", "apache",  xs @ _*) => MergeStrategy.first
//    case PathList("org", "objectweb",  xs @ _*) => MergeStrategy.first
//    case PathList("org",  xs @ _*) => MergeStrategy.first
//    case x => old(x)
//  }
//}

name := "WikiScalding"

//scalaVersion := "2.9.3"

resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "MvnRepository" at "http://mvnrepository.com/artifact")

//"com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.4.2"
//"com.fasterxml.jackson.core" % "jackson-databind" % "2.4.2",
//"com.fasterxml.jackson.core" % "jackson-core" % "2.4.2",
//"org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.3",
libraryDependencies ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.4.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.4.2",
  "com.twitter" %% "scalding-core" % "0.12.0rc3" exclude("org.codehaus.jackson", "jackson-core-asl") exclude("org.codehaus.jackson", "jackson-mapper-asl"),
  "com.twitter" %% "scalding-repl" % "0.12.0rc3" exclude("org.codehaus.jackson", "jackson-core-asl") exclude("org.codehaus.jackson", "jackson-mapper-asl"),
  "org.apache.hadoop" % "hadoop-client" % "2.3.0-mr1-cdh5.0.2"  exclude("org.codehaus.jackson", "jackson-core-asl") exclude("org.codehaus.jackson", "jackson-mapper-asl"))

//lazy val scaldingCore = ProjectRef(uri("https://github.com/twitter/scalding.git"), "scalding-core")

//lazy val scaldingRepl = ProjectRef(uri("https://github.com/twitter/scalding.git"), "scalding-repl")

//lazy val myProject = project in file(".") dependsOn (scaldingCore)
//, scaldingRepl)
