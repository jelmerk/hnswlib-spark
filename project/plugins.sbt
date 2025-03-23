addSbtPlugin("com.eed3si9n"            % "sbt-assembly"  % "2.3.1")
addSbtPlugin("com.github.sbt"          % "sbt-dynver"    % "5.1.0")
addSbtPlugin("com.github.sbt"          % "sbt-pgp"       % "2.3.1")
addSbtPlugin("org.xerial.sbt"          % "sbt-sonatype"  % "3.12.2")
addSbtPlugin("org.scalameta"           % "sbt-scalafmt"  % "2.5.4")
addSbtPlugin("com.thesamet"            % "sbt-protoc"    % "1.0.7")
addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.2.12")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11"
