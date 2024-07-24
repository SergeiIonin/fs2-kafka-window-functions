ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val commonSettings = Seq(
  organization := "io.github.sergeiionin",
  scalaVersion := "2.13.14",
  autoCompilerPlugins := true,
  addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.3" cross CrossVersion.full),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    libraryDependencies := Dependencies.allDependencies ++ Dependencies.allTestDependencies,
    name := "fs2-kafka-window-funcs"
  )

