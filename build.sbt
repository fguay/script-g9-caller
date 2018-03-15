name := "scripts"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies += "com.typesafe.play" %% "play-ahc-ws-standalone" % "1.1.1"
libraryDependencies += "com.typesafe.play" %% "play-ws-standalone-xml" % "1.1.1"
libraryDependencies += "com.typesafe.play" %% "play-ws-standalone-json" % "1.1.1"
libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5", "org.slf4j" % "slf4j-simple" % "1.7.5")

