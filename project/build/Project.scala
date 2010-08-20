import sbt._
import Process._
import sbt_akka_bivy._
class Project(info: ProjectInfo) extends DefaultProject(info) with AkkaProject with AkkaKernelDeployment{
  val akkaCamel = akkaModule("camel")
  val akkaKernel = akkaModule("kernel")
  val junit = "junit" % "junit" % "4.8.1" % "test->default"
  val camelFtp= "org.apache.camel" % "camel-ftp" % "2.4.0" % "compile"
  val camelMina= "org.apache.camel" % "camel-mina" % "2.4.0" % "compile"
  val camelJetty= "org.apache.camel" % "camel-jetty" % "2.4.0" % "compile"
  // for coverage
  val emma = "emma" % "emma" % "2.0.5312" % "test->default"

  val scalatest = "org.scalatest" % "scalatest" % "1.2-for-scala-2.8.0.final-SNAPSHOT" % "test->default"
  override def repositories = Set( "scala-tools-snapshots" at 
      "http://scala-tools.org/repo-snapshots/"
  )
}
