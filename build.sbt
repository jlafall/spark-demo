organization := "spark-demo"

name := "spark-demo"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq (
	"joda-time" % "joda-time" % "2.9.3",
	"org.apache.spark" % "spark-core_2.10" % "1.6.1"
)