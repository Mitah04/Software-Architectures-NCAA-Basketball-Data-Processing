ThisBuild / version := "0.0.1"
ThisBuild / scalaVersion := "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "BasketBall Records"
  )

resolvers += "Akka library repository" at "https://repo.akka.io/maven"

libraryDependencies ++= Seq{
  "com.typesafe.akka" %% "akka-stream-typed" % "2.10.0"
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "8.0.0"
}
