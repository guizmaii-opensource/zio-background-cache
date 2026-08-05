import BuildHelper.{noDoc, stdSettings}

Global / onChangedBuildSource := ReloadOnSourceChanges

ThisBuild / scalaVersion      := "3.3.8"
ThisBuild / scalafmtCheck     := true
ThisBuild / scalafmtSbtCheck  := true
ThisBuild / scalafmtOnCompile := !insideCI.value
ThisBuild / scalafixOnCompile := !insideCI.value
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision // use Scalafix compatible version

// ### Aliases ###

addCommandAlias("tc", "Test/compile")
addCommandAlias("ctc", "clean; tc")
addCommandAlias("rctc", "reload; ctc")
addCommandAlias("fix", "scalafixAll; scalafmtAll; scalafmtSbt")
addCommandAlias("check", "scalafixAll --check; scalafmtCheckAll; scalafmtSbtCheck")

// ### Dependencies ###

lazy val zioVersion              = "2.1.26"
lazy val zioOpenTelemetryVersion = "4.0.0-RC12"

// ### Modules ###

lazy val root =
  Project(id = "zio-background-cache", base = file("."))
    .settings(noDoc *)
    .settings(publish / skip := true)
    .settings(crossScalaVersions := Nil) // https://www.scala-sbt.org/1.x/docs/Cross-Build.html#Cross+building+a+project+statefully,
    .aggregate(core, opentelemetry)

lazy val core =
  project
    .in(file("modules/core"))
    .settings(stdSettings *)
    .settings(
      name := "zio-background-cache-core",
      libraryDependencies ++= Seq(
        "dev.zio" %% "zio"          % zioVersion,
        "dev.zio" %% "zio-test"     % zioVersion % Test,
        "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
      ),
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    )

lazy val opentelemetry =
  project
    .in(file("modules/opentelemetry"))
    .settings(stdSettings *)
    .settings(
      name := "zio-background-cache-opentelemetry",
      libraryDependencies ++= Seq(
        "dev.zio" %% "zio-opentelemetry-core"    % zioOpenTelemetryVersion,
        "dev.zio" %% "zio-opentelemetry-testkit" % zioOpenTelemetryVersion % Test,
        "dev.zio" %% "zio-test"                  % zioVersion              % Test,
        "dev.zio" %% "zio-test-sbt"              % zioVersion              % Test,
      ),
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    )
    .dependsOn(core)

inThisBuild(
  List(
    organization := "com.guizmaii",
    homepage     := Some(url("https://github.com/guizmaii-opensource/zio-background-cache")),
    licenses     := List("Apache 2.0" -> url("https://opensource.org/license/apache-2.0")),
    developers   := List(
      Developer(
        "guizmaii",
        "Jules Ivanic",
        "jules.ivanic@gmail.com",
        url("https://blog.jules-ivanic.com/#/")
      )
    )
  )
)
