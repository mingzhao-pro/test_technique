name := "early_bird"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases"

libraryDependencies ++= Seq(
  "com.storm-enroute" %% "scalameter-core" % "0.6",
  "com.storm-enroute" %% "scalameter" % "0.6" % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "junit" % "junit" % "4.10" % Test
)

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")