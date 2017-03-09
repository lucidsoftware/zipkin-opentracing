autoScalaLibrary := false

crossPaths := false

developers += Developer("pauldraper", "Paul Draper", "paulddraper@gmail.com", url("https://github.com/pauldraper"))

homepage := Some(url("https://git.lucidchart.com/lucidsoftware/opentracing-zipkin"))

javacOptions in (Compile, compile) += "-Xlint:unchecked"

libraryDependencies ++= Seq(
  "io.opentracing" % "opentracing-api" % "0.20.7",
  "io.zipkin.reporter" % "zipkin-reporter" % "0.6.12"
)

licenses += "Apache 2.0 License" -> url("https://www.apache.org/licenses/LICENSE-2.0")

name := "zipkin-opentracing"

organization := "com.lucidchart"

organizationHomepage := Some(url("http://opentracing.io/"))

organizationName := "OpenTracing"

scmInfo := Some(ScmInfo(
  url("https://github.com/lucidsoftware/opentracing-zipkin"),
    "scm:git:git@github.com:lucidsoftware/opentracing-zipkin.git"
  ))

startYear := Some(2017)

version := sys.props.getOrElse("build.version", "0-SNAPSHOT")

inScope(Global)(Seq(
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    sys.env.getOrElse("SONATYPE_USERNAME", ""),
    sys.env.getOrElse("SONATYPE_PASSWORD", "")
  ),
  PgpKeys.pgpPassphrase := Some(Array.emptyCharArray)
))
