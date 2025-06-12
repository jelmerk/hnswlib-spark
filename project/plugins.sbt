addSbtPlugin("com.eed3si9n"            % "sbt-assembly"  % "2.3.1")
addSbtPlugin("com.github.sbt"          % "sbt-dynver"    % "5.1.0")
addSbtPlugin("com.github.sbt"          % "sbt-pgp"       % "2.3.1")
addSbtPlugin("org.xerial.sbt"          % "sbt-sonatype"  % "3.12.2")
addSbtPlugin("org.scalameta"           % "sbt-scalafmt"  % "2.4.6")
addSbtPlugin("com.thesamet"            % "sbt-protoc"    % "1.0.8")
addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.2.13")

if (sys.props.get("sparkVersion").exists(_.startsWith("4."))) {
  libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"
} else {
  libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11"
}
