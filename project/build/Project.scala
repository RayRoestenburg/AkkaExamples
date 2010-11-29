import sbt._
import Process._
import sbt_akka_bivy._
class Project(info: ProjectInfo) extends DefaultProject(info) with AkkaProject with AkkaKernelDeployment{
   val AkkaRepo = "Akka Repository" at "http://scalablesolutions.se/akka/repository"
  val akkaCamel = akkaModule("camel")
  val akkaKernel = akkaModule("kernel")
  val akkaRedis = akkaModule("persistence-redis")
  val junit = "junit" % "junit" % "4.8.1" % "test->default"
  val camelFtp= "org.apache.camel" % "camel-ftp" % "2.5.0" % "compile"
  val camelMina= "org.apache.camel" % "camel-mina" % "2.5.0" % "compile"
  val camelJetty= "org.apache.camel" % "camel-jetty" % "2.5.0" % "compile"
  // for coverage
  val emma = "emma" % "emma" % "2.0.5312" % "test->default"
  val jgroups = "jgroups" % "jgroups" % "2.9.0.GA" % "compile" 
  val scalatest = "org.scalatest" % "scalatest" % "1.2-for-scala-2.8.0.final-SNAPSHOT" % "test->default"
  override def repositories = Set("scala-tools-snapshots" at 
      "http://www.scalablesolutions.se/akka/repository",
      "jboss" at "http://repository.jboss.org/maven2"
  ) 
 
  // dont include integration and performance tests by default.
  override def includeTest(s: String) = {!s.contains("integration.") && !s.contains("performance.")}

  lazy val integrationTest = 
    defaultTestTask(TestListeners(testListeners) ::
		TestFilter(s => s.contains("integration.")) ::
		Nil)

  lazy val performanceTest = 
    defaultTestTask(TestListeners(testListeners) ::
		TestFilter(s => s.contains("performance.")) ::
		Nil)

}
