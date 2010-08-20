package unit.akka

import se.scalablesolutions.akka.actor.Actor
import se.scalablesolutions.akka.actor.Actor._
import org.scalatest.{BeforeAndAfterAll, Spec}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import se.scalablesolutions.akka.remote.{RemoteClient, RemoteServer}
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import unit.test.proto.Commands
import unit.test.proto.Commands.WorkerCommand
import unit.akka.CommandBuilder._

/**
 * Test to check if communicating with protobuf serialized messages works.
 */
@RunWith(classOf[JUnitRunner])
class ProtobufSpecs extends Spec with BeforeAndAfterAll with MustMatchers {
  var server: RemoteServer = new RemoteServer()

  override def beforeAll(configMap: Map[String, Any]) {
    server.start("127.0.0.1", 8091)
  }

  override def afterAll(configMap: Map[String, Any]) {
    RemoteClient.shutdownAll
    server.shutdown
  }

  describe("Send using Protobuf protocol") {

    it("should receive local protobuf pojo and reply") {
      // send a local msg, check if everything is ok
      val actor = actorOf(new TestProtobufWorker())
      actor.start
      val msg = Worker(1, "my-name-1", "my-data-1")
      val result: Option[Any] = actor !! msg
      result match {
        case Some(reply: Commands.WorkerCommand) => {
          // test actor changes name to uppercase
          reply.getName must be === "MY-NAME-1"
        }
        case None => fail("no response")
      }
      actor.stop
    }

    it("should receive remote protobuf pojo and reply") {
      //start a remote server
      val actor = actorOf(new TestProtobufWorker())
      //register the actor that can be remotely accessed
      server.register("protobuftest", actor)
      val msg = Worker(2, "my-name-2", "my-data-2")
      val result: Option[Any] = RemoteClient.actorFor("protobuftest", "127.0.0.1", 8091) !! msg

      result match {
        case Some(reply: Commands.WorkerCommand) => {
          // test actor changes name to uppercase
          reply.getName must be === "MY-NAME-2"
        }
        case None => fail("no response")
      }
      actor.stop
    }
  }
}


/**
 * Actor that sends back the message uppercased.
 */
class TestProtobufWorker extends Actor {
  def receive = {
    case msg: Commands.WorkerCommand => {
      log.info("received protobuf command pojo:" + msg.getId)
      val r = Worker(2, msg.getName.toUpperCase, msg.getData.toUpperCase)
      log.info("sending back a renamed pojo:" + r.getName)
      self.reply(r)
    }
  }
}

/**
 * Shortcuts for creating WorkerCommands
 */
object CommandBuilder {
  def Worker(id: Int, name: String, data: String): WorkerCommand = {
    Commands.WorkerCommand.newBuilder.setId(id).setName(name).setData(data).build
  }

  def Worker(command: WorkerCommand): WorkerCommand = {
    Commands.WorkerCommand.newBuilder.setId(command.getId).setName(command.getName).setData(command.getData).build
  }
}

