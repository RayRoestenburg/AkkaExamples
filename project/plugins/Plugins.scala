import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val bumRepo = "Bum Networks Release Repository" at "http://repo.bumnetworks.com/releases"
  val akkaPlugin = "se.scalablesolutions.akka" % "akka-sbt-plugin" % "1.0-SNAPSHOT"
  val sbtAkkaBivy = "net.evilmonkeylabs" % "sbt-akka-bivy" % "0.2.0"
}
