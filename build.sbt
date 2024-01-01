import Path.relativeTo
import sys.process.*

ThisBuild / organization := "com.github.jelmerk"
ThisBuild / scalaVersion := "2.12.18"

ThisBuild / fork := true

ThisBuild / dynverSonatypeSnapshots := true

ThisBuild / versionScheme := Some("early-semver")

lazy val publishSettings = Seq(
  pomIncludeRepository := { _ => false },

  licenses := Seq("Apache License 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html")),

  homepage := Some(url("https://github.com/jelmerk/hnswlib-spark")),

  scmInfo := Some(ScmInfo(
    url("https://github.com/jelmerk/hnswlib-spark.git"),
    "scm:git@github.com:jelmerk/hnswlib-spark.git"
  )),

  developers := List(
    Developer("jelmerk", "Jelmer Kuperus", "jkuperus@gmail.com", url("https://github.com/jelmerk"))
  ),

  ThisBuild / credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    sys.env.getOrElse("NEXUS_USER", ""),
    sys.env.getOrElse("NEXUS_PASSWORD", "")
  ),

  publishTo := sonatypePublishToBundle.value,
  sonatypeSessionName := s"[sbt-sonatype] ${name.value} ${version.value}"
)

lazy val noPublishSettings =
  publish / skip := true

val hnswLibVersion = "1.1.2"
val sparkVersion = settingKey[String]("Spark version")

lazy val pyTest    = taskKey[Unit]("Run the python tests")

lazy val root = (project in file("."))
  .aggregate(hnswlibSpark)
  .settings(noPublishSettings)

lazy val hnswlibSpark = (project in file("hnswlib-spark"))
  .settings(
    name := s"hnswlib-spark_${sparkVersion.value.split('.').take(2).mkString("_")}",
    publishSettings,
    crossScalaVersions := {
      if (sparkVersion.value >= "3.2.0") {
        Seq("2.12.18", "2.13.10")
      } else if (sparkVersion.value >= "3.0.0") {
        Seq("2.12.18")
      } else {
        Seq("2.12.18", "2.11.12")
      }
    },
    autoScalaLibrary := false,
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "main" / "python",
    Test / unmanagedSourceDirectories += baseDirectory.value / "src" / "test" / "python",
    Compile / packageBin / mappings ++= {
      val base = baseDirectory.value / "src" / "main" / "python"
      val srcs = base ** "*.py"
      srcs pair relativeTo(base)
    },
    Compile / doc / javacOptions ++= {
      Seq("-Xdoclint:none")
    },
    assembly / mainClass := None,
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
    },
    sparkVersion := sys.props.getOrElse("sparkVersion", "3.3.2"),
    pyTest := {
      val log = streams.value.log

      val artifactPath = (Compile / assembly).value.getAbsolutePath
      if (scalaVersion.value == "2.12.18" && sparkVersion.value >= "3.0.0" || scalaVersion.value == "2.11.12") {
        val pythonVersion = if (scalaVersion.value == "2.11.12") "python3.7" else "python3.9"
        val ret = Process(
          Seq("./run-pyspark-tests.sh", sparkVersion.value, pythonVersion),
          cwd = baseDirectory.value,
          extraEnv = "ARTIFACT_PATH" -> artifactPath
        ).!
        require(ret == 0, "Python tests failed")
      } else {
        // pyspark packages support just one version of scala. You cannot use 2.13.x because it ships with 2.12.x jars
        log.info(s"Running pyTests for Scala ${scalaVersion.value} and Spark ${sparkVersion.value} is not supported.")
      }
    },
    test := {
      (Test / test).value
      (Test / pyTest).value
    },
    pyTest := pyTest.dependsOn(assembly).value,
    libraryDependencies ++= Seq(
      "com.github.jelmerk" %  "hnswlib-utils"      % hnswLibVersion,
      "com.github.jelmerk" %  "hnswlib-core-jdk17" % hnswLibVersion,
      "com.github.jelmerk" %% "hnswlib-scala"      % hnswLibVersion,
      "org.apache.spark"   %% "spark-hive"         % sparkVersion.value             % Provided,
      "org.apache.spark"   %% "spark-mllib"        % sparkVersion.value             % Provided,
      "com.holdenkarau"    %% "spark-testing-base" % s"${sparkVersion.value}_1.4.7" % Test,
      "org.scalatest"      %% "scalatest"          % "3.2.17"                       % Test
    )
  )