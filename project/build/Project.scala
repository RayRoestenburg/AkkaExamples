import sbt._
import Process._

class Project(info: ProjectInfo) extends DefaultWebProject(info) with AkkaProject {
  val akkaCamel = akkaModule("camel")
  val akkaKernel = akkaModule("kernel")
  val junit = "junit" % "junit" % "4.8.1" % "test->default"
  val ftpApache = "org.apache.ftpserver" % "ftplet-api" % "1.0.4" % "compile"
  val camelFtp= "org.apache.camel" % "camel-ftp" % "2.4.0" % "compile"
  val camelMina= "org.apache.camel" % "camel-mina" % "2.4.0" % "compile"
  val camelJetty= "org.apache.camel" % "camel-jetty" % "2.4.0" % "compile"

  val scalatest = "org.scalatest" % "scalatest" % "1.2-for-scala-2.8.0.final-SNAPSHOT" % "test->default"
  override def repositories = Set( "scala-tools-snapshots" at 
      "http://scala-tools.org/repo-snapshots/"
  )
}
