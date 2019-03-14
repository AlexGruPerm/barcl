name := "barcl"
version := "0.1"
scalaVersion := "2.11.8"
version := "1.0"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.6.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scala-lang" % "scala-library" % "2.11.8",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)

//for oracle jdbc driver.
//unmanagedJars in Compile := (file("/lib") ** "*.jar").classpath

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x => MergeStrategy.first
}

assemblyJarName in assembly :="ticksloader.jar"

mainClass in (Compile, packageBin) := Some("bcapp.TicksLoader")
mainClass in (Compile, run) := Some("bcapp.TicksLoader")