import sbt._
import sbt_akka_bivy._

class Project(info: ProjectInfo) extends DefaultProject(info) with IdeaProject with AkkaProject with AkkaKernelDeployment{
  val akkaCamel = akkaModule("camel")
  val akkaTestKit = akkaModule("testkit")
  val akkaRemote = akkaModule("remote")
  val junit = "junit" % "junit" % "4.8.1" % "test->default"
  val camelFtp= "org.apache.camel" % "camel-ftp" % "2.5.0" % "compile"
  val camelMina= "org.apache.camel" % "camel-mina" % "2.5.0" % "compile"
  val camelJetty= "org.apache.camel" % "camel-jetty" % "2.5.0" % "compile"
  val jgroups = "jgroups" % "jgroups" % "2.9.0.GA" % "compile"
  val scalatest = "org.scalatest" % "scalatest_2.9.0" % "1.4.1" % "test->default"

  override def repositories = Set(
      "tools-snapshots" at "http://scala-tools.org/repo-snapshots/",
      "jboss" at "http://repository.jboss.org/maven2",
      "fusesource" at "http://repo.fusesource.com/maven2-all"
  )
}
