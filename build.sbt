ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    libraryDependencies := Dependencies.allDependencies ++ Dependencies.allTestDependencies,
    name := "fs2-kafka-window-funcs"
  )
  .settings(Settings.commonSettings)

