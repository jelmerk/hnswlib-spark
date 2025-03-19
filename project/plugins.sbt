addSbtPlugin("com.eed3si9n"            % "sbt-assembly"  % "2.3.1")
addSbtPlugin("com.github.sbt"          % "sbt-dynver"    % "5.0.1")
addSbtPlugin("com.github.sbt"          % "sbt-pgp"       % "2.2.1")
addSbtPlugin("org.xerial.sbt"          % "sbt-sonatype"  % "3.10.0")
addSbtPlugin("org.scalameta"           % "sbt-scalafmt"  % "2.4.6")
addSbtPlugin("com.thesamet"            % "sbt-protoc"    % "1.0.7")
addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.2.10")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11"
