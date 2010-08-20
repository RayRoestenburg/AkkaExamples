package unit.eip.idempotent

import se.scalablesolutions.akka.util.Logging
import org.scalatest.{BeforeAndAfterAll, Spec}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import se.scalablesolutions.akka.actor.Actor._
import java.util.concurrent.{TimeUnit, CyclicBarrier}
import se.scalablesolutions.akka.remote._
import se.scalablesolutions.akka.actor.Actor
import java.lang.String
import java.net.InetSocketAddress
import eip.idempotent._
import unit.test.proto.Commands.WorkerCommand
import unit.test.proto.Commands

/**
 * Testing idempotent receiver
 *
 *
 * val actor = RemoteClient.actorFor("idem", host, port)
 * val repeater = repeater(actor)
 * repeater ! msg
 *
 * val client = repeater("idem", host, port)
 *
 * val remoteActor = idempotent(actorOf(new Actor), "idem", host, port)
 *
 */
@RunWith(classOf[JUnitRunner])
class IdempotentSpecs extends Spec with ShouldMatchers with BeforeAndAfterAll with Logging {


  var server: RemoteServer = null
  var sendingServer: RemoteServer = null
  val port = 8091
  val repeaterPort = 8093
  val host = "127.0.0.1"

  override def beforeAll(configMap: Map[String, Any]) {
    server = new RemoteServer()
    server.start(host, port)
    sendingServer = new RemoteServer()
    sendingServer.start(host, repeaterPort)
    Thread.sleep(1000)
  }

  override def afterAll(configMap: Map[String, Any]) {
    try {
      Thread.sleep(10000)
      server.shutdown
      //sendingServer.shutdown
      Thread.sleep(1000)
    } catch {
      case e => ()
    } finally {
      RemoteClient.shutdownAll
    }
  }


  describe("Send messages to idempotent receiver") {
    it("should ignore a duplicate message") {
      val client = RemoteClient.clientFor(host, port)
      try {
        val envelopeId = 1L
        val envelopes = new MemEnvelopes()
        envelopes.pending(new Envelope(envelopeId, 1000, new Sender(host, repeaterPort, "repeater")))
        val barrier = new CyclicBarrier(2)
        val rec = actorOf(new Receiver(barrier))
        rec.start
        val receiver = actorOf(new IdempotentReceiver(Set(rec), envelopes))
        server.register("idem", receiver)

        val msg = Worker(1,"command-tracked","my-data-tracked")
        val trackedMessage = EnvelopeSerializer.serialize(new Envelope(envelopeId, 1001, new Sender(host, repeaterPort, "repeater")), msg)
        val idempotentReceiverRef = RemoteClient.actorFor("idem", host, port)
        val repeater = actorOf(new Repeater(Set(idempotentReceiverRef)))
        repeater.start
        sendingServer.register("repeater", repeater)
        client.registerListener(repeater)
        val reply = idempotentReceiverRef !! trackedMessage
        reply match {
          case None => fail
          case Some(response: IgnoredEnvelope) => response.id should equal(envelopeId)
        }
        val countReply = rec !! new CountRequest
        countReply match {
          case None => fail
          case Some(response: CountResponse) => response.count should equal(0)
        }
        client.deregisterListener(repeater)
      } finally {
        RemoteClient.shutdownClientFor(new InetSocketAddress(host, port))
      }
    }
    it("should not ignore a new message") {
      val client = RemoteClient.clientFor(host, port)
      try {
        val existingEnvelopeId = 1L
        val envelopes = new MemEnvelopes()
        envelopes.pending(new Envelope(existingEnvelopeId, 1000, new Sender(host, repeaterPort, "repeater")))
        val barrier = new CyclicBarrier(2)
        val rec = actorOf(new Receiver(barrier))
        rec.start
        val receiver = actorOf(new IdempotentReceiver(Set(rec), envelopes))
        server.register("idem", receiver)

        val msg = Worker(1,"command-tracked","my-data-tracked")
        val newEnvelopeId = 2L
        val trackedMessage = EnvelopeSerializer.serialize(new Envelope(newEnvelopeId, 1001, new Sender(host, repeaterPort, "repeater")), msg)

        val idempotentReceiverRef = RemoteClient.actorFor("idem", host, port)
        val repeater = actorOf(new Repeater(Set(idempotentReceiverRef)))
        repeater.start
        sendingServer.register("repeater", repeater)

        client.registerListener(repeater)
        idempotentReceiverRef ! trackedMessage
        barrier.await(5, TimeUnit.SECONDS)

        val countReply = rec !! new CountRequest
        countReply match {
          case None => fail
          case Some(response: CountResponse) => response.count should equal(1)
        }
        client.deregisterListener(repeater)
      } finally {
        RemoteClient.shutdownClientFor(new InetSocketAddress(host, port))
      }
    }
    it("should repeat after disconnect and handle duplicates correctly") {
      // start a repeater
      // send messages
      // start an idempotent receiver
      // shutdown the idempotent receiver
      // keep sending messages from repeater
      // notice that repeater is buffering
      // restart idempotent receiver
      // notice that repeater is repeating, until buffer empty
      // notice that idempotent receiver is receiving all messages and has handled possible duplicates
    }
    it("should do the same when using the idempotent and repeater object methods") {
      import Repeater.{repeater, tracked}
      import IdempotentReceiver.idempotent
      try {
        val existingEnvelopeId = 1L
        val envelopes = new MemEnvelopes()
        envelopes.pending(new Envelope(existingEnvelopeId, 1000, new Sender(host, repeaterPort, "repeater")))
        val barrier = new CyclicBarrier(2)
        val rec = actorOf(new Receiver(barrier))

        val receiver = idempotent("idem", rec, envelopes)
        val sender = repeater("idem", IdempotentReceiver.host, IdempotentReceiver.port)
        sender ! tracked(Worker(1,"command-tracked","my-data-tracked"))
        barrier.await(5, TimeUnit.SECONDS)
        val countReply = rec !! new CountRequest
        countReply match {
          case None => fail
          case Some(response: CountResponse) => response.count should equal(1)
        }
      }
      finally {
        IdempotentReceiver.server.shutdown
        //Repeater.server.shutdown
        RemoteClient.shutdownClientFor(new InetSocketAddress(host, port))
      }
    }
  }

  def Worker(id: Int, name: String, data: String): WorkerCommand = {
    Commands.WorkerCommand.newBuilder.setId(id).setName(name).setData(data).build
  }

  def Worker(command: WorkerCommand): WorkerCommand = {
    Commands.WorkerCommand.newBuilder.setId(command.getId).setName(command.getName).setData(command.getData).build
  }
}

class CountRequest
case class CountResponse(count: Int)

/**
 * Test Actor that receives messages that are passed through the IdempotentReceiver
 */
class Receiver(barrier: CyclicBarrier) extends Actor {
  var count: Int = 0

  def receive = {
    case msg: CountRequest => {
      self.reply(new CountResponse(count))
    }
    case msg: WorkerCommand => {
      log.info("received message in test receiver:" + msg.getData)
      count += 1
      barrier.await()
    }
  }
}

