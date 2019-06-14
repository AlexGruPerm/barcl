name := "barcl"
scalaVersion := "2.12.4"
version := "1.0"

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.7.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scala-lang" % "scala-library" % "2.12.4",
  //"com.madhukaraphatak" %% "java-sizeof" % "0.1", unresolved for 2.12 scala
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "com.typesafe" % "config" % "1.3.4"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "logback.xml" => MergeStrategy.last
  case "resources/logback.xml" => MergeStrategy.last
  case "application.conf" => MergeStrategy.last
  case PathList("reference.conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}

/**
  * For ticksloader
  *
  * assemblyJarName in assembly :="ticksloader.jar"
  * mainClass in (Compile, packageBin) := Some("bcapp.TicksLoader")
  * mainClass in (Compile, run) := Some("bcapp.TicksLoader")
  *
*/

/**
  * For bar calcluator
  *
  * assemblyJarName in assembly :="barcl.jar"
  * mainClass in (Compile, packageBin) := Some("bcapp.BarsCalcApp")
  * mainClass in (Compile, run) := Some("bcapp.BarsCalcApp")
  *
  */

//sbt -mem 2048 run
//export _JAVA_OPTIONS="-Xms1024m -Xmx2G -Xss256m -XX:MaxPermSize=4G"
//to ~/.bash_profile and restart terminal

/*
assemblyJarName in assembly :="ticksloader.jar"
mainClass in (Compile, packageBin) := Some("bcapp.TicksLoader")
mainClass in (Compile, run) := Some("bcapp.TicksLoader")
*/

assemblyJarName in assembly :="barcl.jar"
mainClass in (Compile, packageBin) := Some("bcapp.BarsCalcApp")
mainClass in (Compile, run) := Some("bcapp.BarsCalcApp")

/*
assemblyJarName in assembly :="facalc.jar"
mainClass in (Compile, packageBin) := Some("bcapp.BarsRanger")
mainClass in (Compile, run) := Some("bcapp.BarsRanger")
*/
/*
assemblyJarName in assembly :="formcalc.jar"
mainClass in (Compile, packageBin) := Some("bcapp.FormsCalcApp")
mainClass in (Compile, run) := Some("bcapp.FormsCalcApp")
*/
