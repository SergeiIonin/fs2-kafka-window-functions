import sbt._

object Dependencies {

  private object Kafka {
    val kafkaCore =  "org.apache.kafka" %% "kafka" % "3.1.0"
    val fs2Kafka =  "com.github.fd4s" %% "fs2-kafka" % "3.0.0-M8" // 2.5.0
    val testContainersVersion = "0.40.9"

    val testContainersScalaCore = "com.dimafeng" %% "testcontainers-scala-core" % testContainersVersion
    val testContainersScalaKafka = "com.dimafeng" %% "testcontainers-scala-kafka" % testContainersVersion


    val deps = Seq(kafkaCore, fs2Kafka, testContainersScalaKafka)
  }

  private object Cats {

      private val kindProjectorVersion = "0.13.2"
      private val catsCoreVersion = "2.7.0"
      private val log4catsSlf4jVersion = "2.2.0"
      private val taglessVersion = "0.14.0"
      private val catsEffectVersion = "3.4.8"
      private val catsMtlVersion = "1.2.1"

      private val kindProjector = "org.typelevel" %% "kind-projector" % kindProjectorVersion
      private val catsCore = "org.typelevel" %% "cats-core" % catsCoreVersion
      private val catsEffect = "org.typelevel" %% "cats-effect" % catsEffectVersion
      private val catsMtl = "org.typelevel" %% "cats-mtl" % catsMtlVersion
      //private val log4catsSlf4j = "org.typelevel" %% "log4cats-slf4j" % log4catsSlf4jVersion
      private val tagless = "org.typelevel" %% "cats-tagless-macros" % taglessVersion

    val deps = Seq(catsCore, catsEffect, tagless, catsMtl)
  }

  private object Http {
    private val http4sVersion = "0.23.1"
    private val http4sArtifacts = List("dsl", "ember-server", "ember-client", "circe")

    def http4s(artifact: String) = "org.http4s" %% s"http4s-$artifact" % http4sVersion

    private val http4sDeps: List[ModuleID] = http4sArtifacts.map(http4s)

    val deps = http4sDeps
  }

  private object OpenSearch {
    /*
    private val restClientVersion = "2.5.0"
    private val openSearchJavaVersion = "2.0.0"

    def openSearch(artifact: String, version: String) =
      "org.opensearch.client" % s"opensearch-$artifact" % version

    val deps = List(openSearch("rest-client", restClientVersion),
                    openSearch("java", openSearchJavaVersion))*/
    val openSearch = "org.opensearch.client" % "opensearch-rest-high-level-client" % "1.3.2"

    val deps = List(openSearch)
  }

  private object Json {
    private val circeVersion = "0.14.4"
    def circe(artifact: String) = "io.circe" %% s"circe-$artifact" % circeVersion

    private val circeDeps = List("core", "generic", "parser").map(circe)
    val deps = circeDeps
  }

  private object Misc {
    private val typesafeConfigVersion = "1.4.2"
    private val wvletVersion = "23.2.2"
    private val slf4jjdk14Version = "2.0.6"

    /*private val pureConfigVersion = "0.17.1"

    val pureConfig = "com.github.pureconfig" %% "pureconfig" % pureConfigVersion

    val deps = Seq(pureConfig)*/

    val typesafeConfig = "com.typesafe" % "config" % "1.4.2"

    //Logs
    val wvlet = "org.wvlet.airframe" %% "airframe-log" % wvletVersion
    val slf4jjdk14 = "org.slf4j" % "slf4j-jdk14"   % slf4jjdk14Version

    val deps = Seq(typesafeConfig, wvlet, slf4jjdk14)
  }

  private object Testing {
      val scalaTest = "org.scalatest" %% "scalatest" % "3.2.10"
      val scalaTestPlusCheck = "org.scalatestplus" %% "scalacheck-1-15" % "3.2.10.0"
      val scalacheck = "org.scalacheck" %% "scalacheck" % "1.15.4"
      val scalacheckCats = "io.chrisdavenport" %% "cats-scalacheck" % "0.3.1"
      val scalacheckEffect = "org.typelevel" %% "scalacheck-effect" % "1.0.3"
      val scalaTestCatEffect = "org.typelevel" %% "cats-effect-testing-scalatest" % "1.4.0"
      val scalaMock = "org.scalamock" %% "scalamock" % "5.2.0"

      val deps =
        Seq(scalaTest, scalacheck, scalaTestPlusCheck, scalacheckCats, scalacheckEffect, scalaTestCatEffect, scalaMock)
    }

  val allDependencies     = Kafka.deps ++ Cats.deps ++ Http.deps ++ Json.deps ++ OpenSearch.deps ++ Misc.deps
  val allTestDependencies = Testing.deps.map(_ % Test)

}
