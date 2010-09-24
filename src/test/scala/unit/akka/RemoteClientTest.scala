package unit.akka

import se.scalablesolutions.akka.util.Logging
import org.scalatest.{Spec, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import se.scalablesolutions.akka.remote.{RemoteClient, RemoteServer}
import se.scalablesolutions.akka.actor.Actor._
import se.scalablesolutions.akka.actor.Actor

class RemoteClientTest extends Spec with ShouldMatchers with BeforeAndAfterAll with Logging {
  val server = new RemoteServer

  override def beforeAll(configMap: Map[String, Any]) {
    server.start("localhost", 11000)
    server.register("test", actorOf(new TestActor))
  }

  override def afterAll(configMap: Map[String, Any]) {
    try {
      server.shutdown
    } catch {
      case e => ()
    } finally {
      RemoteClient.shutdownAll
    }
  }

  describe("when sending to a remoteclient") {
    it("should not get exceptions") {
      val ref = RemoteClient.actorFor("test", "localhost", 11000)
      ref ! "hello"
      Thread.sleep(1000)
      val reply = ref !! "hello"
      reply match {
        case Some(msg:String) => msg should be ("hello")
        case None => fail("no reply")
      }
    }
  }

}
class TestActor extends Actor {
  def receive = {
    case msg: Any => {
        log.info("msg: %s",msg.toString)
        self.reply_?(msg)
    }
  }
}