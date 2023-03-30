addSbtPlugin("com.github.sbt"    % "sbt-git"             % "2.0.0")
addSbtPlugin(
  "com.github.sbt"               % "sbt-native-packager" % "1.9.11" excludeAll (ExclusionRule(
    organization = "org.scala-lang.modules",
    name = "scala-xml_2.12",
  ))
)
addSbtPlugin("se.marcuslonnberg" % "sbt-docker"          % "1.9.0")
addSbtPlugin("com.eed3si9n"      % "sbt-buildinfo"       % "0.11.0")
addSbtPlugin("org.scalameta"     % "sbt-scalafmt"        % "2.4.6")
addSbtPlugin("org.latestbit"     % "sbt-gcs-plugin"      % "1.7.1")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.28")
