val commonSettings = Seq(
  scalaVersion := "2.13.5",
  parallelExecution in Test := false,
  scalacOptions ++= Seq(
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-encoding",
    "utf-8", // Specify character encoding used by source files.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-language:postfixOps",
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-Xfatal-warnings", // Fail the compilation if there are any warnings.
    "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
    "-Xlint:constant", // Evaluation of a constant arithmetic expression results in an error.
    "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
    "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
    "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
    "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
    "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
    "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
    "-Xlint:option-implicit", // Option.apply used implicit view.
    "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
    "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
    "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
    "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
    // "-Ywarn-dead-code", // Warn when dead code is identified.
    "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
    "-Ywarn-numeric-widen", // Warn when numerics are widened.
    "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
    "-Ywarn-unused:locals", // Warn if a local definition is unused.
    "-Ywarn-unused:params", // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
    "-Ywarn-unused:privates" // Warn if a private member is unused.
  ),
  organization := "io.github.pityka",
  licenses += ("MIT", url("https://opensource.org/licenses/MIT")),
  publishTo := sonatypePublishTo.value,
  pomExtra in Global := {
    <url>https://pityka.github.io/lame/</url>
        <scm>
          <connection>scm:git:github.com/pityka/lame</connection>
          <developerConnection>scm:git:git@github.com:pityka/lame</developerConnection>
          <url>github.com/pityka/lame</url>
        </scm>
        <developers>
          <developer>
            <id>pityka</id>
            <name>Istvan Bartha</name>
            <url>https://pityka.github.io/lame/</url>
          </developer>
        </developers>
  },
  fork := true,
  cancelable in Global := true,
  version := "0.0.2"
)

commonSettings

publishArtifact := false

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "lame",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % "2.5.23",
      "org.scalatest" %% "scalatest" % "3.2.5" % Test,
      "com.github.samtools" % "htsjdk" % "2.19.0" % Test
    )
  )

lazy val bgzipIndex = (project in file("bgzip-index"))
  .settings(commonSettings: _*)
  .settings(
    name := "lame-bgzip-index",
    libraryDependencies ++= Seq(
      "io.github.pityka" %% "intervaltree" % "1.1.2",
      "org.scalatest" %% "scalatest" % "3.2.5" % "test"
    )
  )
  .dependsOn(core)

lazy val root = (project in file(".")).aggregate(core, bgzipIndex)
