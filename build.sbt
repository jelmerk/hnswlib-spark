import scalapb.compiler.Version.grpcJavaVersion
import scalapb.compiler.Version.scalapbVersion
import sys.process.*

ThisBuild / organization := "com.github.jelmerk"
ThisBuild / scalaVersion := "2.13.16"

ThisBuild / fork := true

ThisBuild / Test / parallelExecution := false

ThisBuild / dynverSonatypeSnapshots := true

ThisBuild / javaOptions := Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
//  "-Xfatal-warnings",
  "-Wconf:msg=higherKinds.*:silent",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused",
  "-target:jvm-1.8",
  "-encoding", "UTF-8"
)

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"

ThisBuild / scapegoatIgnoredFiles := Seq(".*/src_managed/.*")

lazy val publishSettings = Seq(
  pomIncludeRepository := { _ => false },
  licenses             := Seq("Apache License 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html")),
  homepage             := Some(url("https://github.com/jelmerk/hnswlib-spark")),
  scmInfo              := Some(
    ScmInfo(
      url("https://github.com/jelmerk/hnswlib-spark.git"),
      "scm:git@github.com:jelmerk/hnswlib-spark.git"
    )
  ),
  developers := List(
    Developer("jelmerk", "Jelmer Kuperus", "jkuperus@gmail.com", url("https://github.com/jelmerk"))
  ),
  ThisBuild / credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    sys.env.getOrElse("NEXUS_USER", ""),
    sys.env.getOrElse("NEXUS_PASSWORD", "")
  ),
  publishTo           := sonatypePublishToBundle.value,
  sonatypeSessionName := s"[sbt-sonatype] ${name.value} ${version.value}"
)

lazy val noPublishSettings =
  publish / skip := true

val hnswLibVersion = "1.2.1"
val sparkVersion   = settingKey[String]("Spark version")
val venvFolder     = settingKey[String]("Venv folder")
val pythonVersion  = settingKey[String]("Python version")

lazy val createVirtualEnv = taskKey[Unit]("Create venv")
lazy val pyTest           = taskKey[Unit]("Run the python tests")
lazy val black            = taskKey[Unit]("Run the black code formatter")
lazy val blackCheck       = taskKey[Unit]("Run the black code formatter in check mode")
lazy val flake8           = taskKey[Unit]("Run the flake8 style enforcer")
lazy val pyPackage        = taskKey[Unit]("Package python code")
lazy val pyPublish        = taskKey[Unit]("Publish python code")

lazy val root = (project in file("."))
  .aggregate(uberJar, cosmetic)
  .settings(noPublishSettings)

lazy val uberJar = (project in file("hnswlib-spark"))
  .settings(
    name := s"hnswlib-spark-uberjar_${sparkVersion.value.split('.').take(2).mkString("_")}",
    noPublishSettings,
    crossScalaVersions := {
      if (sparkVersion.value >= "4.0.0") {
        Seq("2.13.16")
      } else {
        Seq("2.12.20", "2.13.16")
      }
    },
    autoScalaLibrary   := false,
    Compile / unmanagedResourceDirectories += baseDirectory.value / "src" / "main" / "python",
    Compile / unmanagedResources / includeFilter := {
      val pythonSrcDir = baseDirectory.value / "src" / "main" / "python"
      (file: File) => {
        if (file.getAbsolutePath.startsWith(pythonSrcDir.getAbsolutePath)) file.getName.endsWith(".py")
        else true
      }
    },
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "main" / "python",
    Test / unmanagedSourceDirectories += baseDirectory.value / "src" / "test" / "python",
    Test / envVars += "SPARK_TESTING" -> "1",
    Compile / doc / scalacOptions ++= Seq(
      // it's not easy to make generated proto code protected so just exclude it
      "-skip-packages",
      "com.github.jelmerk.index:com.github.jelmerk.registration",
      "-doc-footer",
      s"Hnswlib spark v.${version.value}"
    ),
    apiMappings ++= {
      Option(System.getProperty("sun.boot.class.path"))
        .flatMap { classPath =>
          classPath.split(java.io.File.pathSeparator).find(_.endsWith(java.io.File.separator + "rt.jar"))
        }
        .map { jarPath =>
          Map(
            file(jarPath) -> url("https://docs.oracle.com/javase/8/docs/api")
          )
        }
        .getOrElse {
          streams.value.log.warn("Failed to add bootstrap class path of Java to apiMappings")
          Map.empty[File, URL]
        }
    },
    assembly / mainClass := None,
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
    },
    cleanFiles += baseDirectory.value / "dist",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case x                                                    =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("google.**" -> "shaded.google.@1").inAll,
      ShadeRule.rename("com.google.**" -> "shaded.com.google.@1").inAll,
      ShadeRule.rename("io.grpc.**" -> "shaded.io.grpc.@1").inAll,
      ShadeRule.rename("io.netty.**" -> "shaded.io.netty.@1").inAll
    ),
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    ),
    sparkVersion     := sys.props.getOrElse("sparkVersion", "3.5.5"),
    venvFolder       := s"${baseDirectory.value}/.venv",
    pythonVersion    := "python3.9",
    createVirtualEnv := {
      val ret = (
        s"${pythonVersion.value} -m venv ${venvFolder.value}" #&&
          s"${venvFolder.value}/bin/pip install wheel==0.42.0 pytest==7.4.3 pyspark[ml]==${sparkVersion.value} black==23.3.0 flake8==5.0.4 build==1.2.2.post1 twine==6.0.1"
      ).!
      require(ret == 0, "Creating venv failed")
    },
    pyTest := {
      val log = streams.value.log

      val artifactPath = (Compile / assembly).value.getAbsolutePath
      val venv         = venvFolder.value

      if (scalaVersion.value == "2.12.20" && sparkVersion.value < "4.0.0" || sparkVersion.value >= "4.0.0") {
        val ret = Process(
          Seq(s"$venv/bin/pytest", "--junitxml=target/test-reports/TEST-python.xml", "src/test/python"),
          cwd = baseDirectory.value,
          extraEnv = "ARTIFACT_PATH" -> artifactPath,
          "PYTHONPATH"    -> s"${baseDirectory.value}/src/main/python",
          "SPARK_TESTING" -> "1"
        ).!
        require(ret == 0, "Python tests failed")
      } else {
        // pyspark packages support just one version of scala. You cannot use 2.13.x because it ships with 2.12.x jars
        log.info(s"Running pyTests for Scala ${scalaVersion.value} and Spark ${sparkVersion.value} is not supported.")
      }
    },
    pyTest     := pyTest.dependsOn(assembly, createVirtualEnv).value,
    blackCheck := {
      val ret = s"${venvFolder.value}/bin/black --check ${baseDirectory.value}/src/main/python".!
      require(ret == 0, "Black failed")
    },
    blackCheck := blackCheck.dependsOn(createVirtualEnv).value,
    black      := {
      val ret = s"${venvFolder.value}/bin/black ${baseDirectory.value}/src/main/python".!
      require(ret == 0, "Black failed")
    },
    black  := black.dependsOn(createVirtualEnv).value,
    flake8 := {
      val ret = s"${venvFolder.value}/bin/flake8 ${baseDirectory.value}/src/main/python".!
      require(ret == 0, "Flake8 failed")
    },
    flake8    := flake8.dependsOn(createVirtualEnv).value,
    pyPackage := {
      val venv = venvFolder.value
      val ret  = Process(
        Seq(s"$venv/bin/python", "-m", "build"),
        cwd = baseDirectory.value,
        extraEnv = "VERSION" -> version.value
      ).!
      require(ret == 0, "Build failed")
    },
    pyPackage := pyPackage.dependsOn(createVirtualEnv).value,
    pyPublish := {
      val venv = venvFolder.value
      val ret  = Process(
        Seq(s"$venv/bin/python", "-m", "twine", "upload", "dist/*"),
        cwd = baseDirectory.value
      ).!
      require(ret == 0, "PyPublish failed")
    },
    pyPublish := pyPublish.dependsOn(pyPackage).value,
    libraryDependencies ++= Seq(
      "com.github.jelmerk"    % "hnswlib-utils"        % hnswLibVersion,
      "com.github.jelmerk"    % "hnswlib-core-jdk17"   % hnswLibVersion,
      "com.github.jelmerk"   %% "hnswlib-scala"        % hnswLibVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime"      % scalapbVersion     % "protobuf",
      "io.grpc"               % "grpc-netty"           % grpcJavaVersion,
      "org.apache.spark"     %% "spark-hive"           % sparkVersion.value % Provided,
      "org.apache.spark"     %% "spark-mllib"          % sparkVersion.value % Provided,
      "org.scalatest"        %% "scalatest"            % "3.2.19"           % Test
    )
  )

// spark cannot resolve artifacts with classifiers so we replace the main artifact
//
// See: https://issues.apache.org/jira/browse/SPARK-20075
// See: https://github.com/sbt/sbt-assembly/blob/develop/README.md#q-despite-the-concerned-friends-i-still-want-publish-%C3%BCber-jars-what-advice-do-you-have
lazy val cosmetic = project
  .settings(
    name                 := s"hnswlib-spark_${sparkVersion.value.split('.').take(2).mkString("_")}",
    Compile / packageBin := (uberJar / assembly).value,
    Compile / packageDoc := (uberJar / Compile / packageDoc).value,
    Compile / packageSrc := (uberJar / Compile / packageSrc).value,
    autoScalaLibrary     := false,
    crossScalaVersions := {
      if (sparkVersion.value >= "4.0.0") {
        Seq("2.13.16")
      } else {
        Seq("2.12.20", "2.13.16")
      }
    },
    sparkVersion         := sys.props.getOrElse("sparkVersion", "3.5.5"),
    publishSettings
  )
