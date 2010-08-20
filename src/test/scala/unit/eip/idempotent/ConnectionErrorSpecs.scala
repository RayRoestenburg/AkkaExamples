package unit.eip.idempotent

import org.scalatest.matchers.ShouldMatchers
import se.scalablesolutions.akka.util.Logging
import org.scalatest.{BeforeAndAfterAll, Spec}
import tools.NetworkProxy
import se.scalablesolutions.akka.actor.Actor._
import collection.mutable.HashMap
import java.util.concurrent.{TimeUnit, CyclicBarrier}
import se.scalablesolutions.akka.actor.{ActorRef, Actor}
import scala.actors
import se.scalablesolutions.akka.remote._

/**
 * Test what happens in case of Connection Errors, using a simple Network Proxy that disconnects 'the network'
 * between client and server
 */
class ConnectionErrorSpecs extends Spec with ShouldMatchers with BeforeAndAfterAll with Logging {
  val server = new RemoteServer()
  val proxy = new NetworkProxy("localhost", 18000, 18095)
  val barrier = new CyclicBarrier(2)
  var actorRef: ActorRef = null
  var serverListener: ActorRef = null
  var clientListener: ActorRef = null
  val client: RemoteClient = null

  override def beforeAll(configMap: Map[String, Any]) {
    serverListener = actorOf(new ConnectionListenerActor())
    serverListener.start
    server.addListener(serverListener)
    server.start("localhost", 18095)
    proxy.start
    server.register("test", actorOf(new ConnTestActor(barrier)))
    actorRef = RemoteClient.actorFor("test", "localhost", 18000)
    val client = RemoteClient.clientFor("localhost", 18000)
    clientListener = actorOf(new ConnectionListenerActor())
    clientListener.start
    client.addListener(clientListener);
  }

  override def afterAll(configMap: Map[String, Any]) {
    try {
      if(server.isRunning){
        server.shutdown
      }
    } catch {
      case e => ()
    } finally {
      RemoteClient.shutdownAll
      log.info("remote client and server shutdown complete")
    }
  }

  describe("Remote client sends message to Remote Server through network proxy") {
    it("should send and receive throught the proxy") {
      actorRef ! new TestOneWay("test")
      barrier.await(1000, TimeUnit.MILLISECONDS)
      assertReply(actorRef)
    }
    it("should not receive when proxy is stopped (network disconnected)") {
      proxy.stop
      actorRef ! new TestOneWay("test")
      assertNoReply(actorRef)
    }
    it("should receive a reply when a request is made after the connection is back again (proxy back up)") {
      proxy.start
      //wait for a little while because the reconnect takes some time.
      Thread.sleep(20000)
      assertReply(actorRef)
      proxy.stop
    }
    it("should notify the connection listener of events on the client connection") {
      var reply: Option[Any] = clientListener !!  new CountOneWayRequests("client-connect")
      assertAtLeastOneReply(reply)
      reply= clientListener !!  new CountOneWayRequests("client-disconnect")
      assertAtLeastOneReply(reply)
      reply= serverListener !!  new CountOneWayRequests("server-started")
      assertAtLeastOneReply(reply)
      //reply= clientListener !!  new CountOneWayRequests("client-error")
      //assertAtLeastOneReply(reply)
      //reply= serverListener !!  new CountOneWayRequests("server-error")
      //assertAtLeastOneReply(reply)
      server.shutdown
      reply= serverListener !!  new CountOneWayRequests("server-shutdown")
      assertAtLeastOneReply(reply)
    }
  }

  def assertAtLeastOneReply (reply: Option[Any]) = {
    reply match {
      case Some(response: CountOneWayResponse) => {
        response.count should be > (0)
      }
      case None => fail("no reply")
    }
  }
  def assertReply(actorRef: ActorRef) = {
    var reply: Option[Any] = actorRef !! new CountOneWayRequests("test")
    reply match {
      case Some(response: CountOneWayResponse) => {
        response.count should equal(1)
        log.info("received reply correctly")

      }
      case None => fail("no reply")
    }
  }

  def assertNoReply(actorRef: ActorRef) = {
    val reply = actorRef !! new CountOneWayRequests("test")
    reply match {
      case Some(response: CountOneWayResponse) => {
        fail("reply when proxy was stopped")
      }
      case None => log.info("no response from actor when proxy stopped.")
    }
  }

}
case class TestOneWay(data: String)
case class CountOneWayRequests(data: String)
case class CountOneWayResponse(count: Int)
case class TestRequest(data: String)
case class TestResponse(data: String)

class ConnTestActor(barrier: CyclicBarrier) extends Actor {
  val map = new HashMap[String, Int]

  def receive = {
    case msg: TestOneWay => {
      if (map.contains(msg.data)) {
        map(msg.data) = map(msg.data) + 1
      } else {
        map += msg.data -> 1
      }
      barrier.await
    }
    case msg: CountOneWayRequests => {
      self.reply(new CountOneWayResponse(map(msg.data)))
    }
    case msg: TestRequest => {
      self.reply(new TestResponse(msg.data))
    }
  }
}

class ConnectionListenerActor extends Actor {
  val map = new HashMap[String, Int]
  map+= "error" -> 0
  map+= "disconnect" -> 0
  map+= "connect" -> 0
  def countEvent(event: String): Unit = {
    if (map.contains(event)) {
      map(event) = map(event) + 1
    } else {
      map += event -> 1
    }
  }

  def receive = {
    case RemoteClientError(cause, hostname, port) => {
      log.info("listener: client error on %s:%s", hostname, port)
      countEvent("client-error")
    }
    case RemoteClientDisconnected(hostname, port) => {
      log.info("listener: client disconnect on %s:%s", hostname, port)
      countEvent("client-disconnect")
    }
    case RemoteClientConnected(hostname, port) => {
      log.info("listener: client connect on %s:%s", hostname, port)
      countEvent("client-connect")
    }
    case RemoteServerError(cause, hostname, port) => {
      log.info("listener: server error on %s:%s", hostname, port)
      countEvent("server-error")
    }
    case RemoteServerShutdown(hostname, port) => {
      log.info("listener: server shutdown on %s:%s", hostname, port)
      countEvent("server-shutdown")
    }
    case RemoteServerStarted(hostname, port) => {
      log.info("listener: server started on %s:%s", hostname, port)
      countEvent("server-started")
    }
    case msg: CountOneWayRequests => {
      self.reply(new CountOneWayResponse(map(msg.data)))
    }
  }
}