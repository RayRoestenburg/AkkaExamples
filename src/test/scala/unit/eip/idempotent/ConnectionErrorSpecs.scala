package unit.eip.idempotent

import org.scalatest.matchers.ShouldMatchers
import akka.util.Logging
import org.scalatest.{BeforeAndAfterAll, Spec}
import tools.NetworkProxy
import akka.actor.Actor._
import collection.mutable.HashMap
import java.util.concurrent.{TimeUnit, CyclicBarrier}
import akka.actor.{ActorRef, Actor}
import akka.remote._
import java.io.{InputStream, OutputStream}
import java.net.{InetSocketAddress, Socket}

/**
 * Test what happens in case of Connection Errors, using a simple Network Proxy that is used to disconnect 'the network'
 * between client and server, or cause problems.
 */
class ConnectionErrorSpecs extends Spec with ShouldMatchers with BeforeAndAfterAll with Logging {
  val server = new RemoteServer()
  val proxy = new NetworkProxy("localhost", 18000, 18095)
  val barrier = new CyclicBarrier(2)
  var actorRef: ActorRef = null
  var serverListener: ActorRef = null
  var clientListener: ActorRef = null
  var client: RemoteClient = null

  override def beforeAll(configMap: Map[String, Any]) {
    serverListener = actorOf(new ConnectionListenerActor())
    serverListener.start
    server.addListener(serverListener)
    server.start("localhost", 18095)
    proxy.start
    server.register("test", actorOf(new ConnTestActor(barrier)))
    actorRef = RemoteClient.actorFor("test", "localhost", 18000)
    client = RemoteClient.clientFor("localhost", 18000)
    clientListener = actorOf(new ConnectionListenerActor())
    clientListener.start
    client.addListener(clientListener);
  }

  override def afterAll(configMap: Map[String, Any]) {
    try {
      if (server.isRunning) {
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
    it("should send and receive through the proxy") {
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
      //wait for a little while because the reconnect takes some time to happen.
      Thread.sleep(5000)
      assertReply(actorRef)

    }
    it("should not get a reply at client error") {
      def clientClientProblem(client: Socket, server: Socket, in: InputStream, out: OutputStream): Unit = {
      }
      def serverClientProblem(client: Socket, server: Socket, in: InputStream, out: OutputStream): Unit = {
        //force broken pipe
        client.close
      }
      // the two injected functions cause a client side error
      proxy.injectClientFunction(clientClientProblem)
      proxy.injectServerFunction(serverClientProblem)
      // trigger problem
      assertNoReply(actorRef)
      assertNoReply(actorRef)
      proxy.clearInjectedFunctions
    }
    it("should notify the connection listener of client-error") {
      val reply = clientListener !! new CountOneWayRequests("client-error")
      assertAtLeastOneReply(reply)
    }
    
    it("should not get a reply at server error") {
      def clientServerProblem(client: Socket, server: Socket, in: InputStream, out: OutputStream): Unit = {
        //write some garbage to server
        in.close()
        out.write(Array[Byte](10,13,10))
        out.flush
        out.close
      }
      def serverServerProblem(client: Socket, server: Socket, in: InputStream, out: OutputStream): Unit = {
        // cause some more problems
        in.close()
        out.close()
      }
      // the two injected functions cause a server side error
      proxy.injectClientFunction(clientServerProblem)
      proxy.injectServerFunction(serverServerProblem)
      // trigger problem
      assertNoReply(actorRef)
      assertNoReply(actorRef)
      proxy.clearInjectedFunctions
    }
    it("should notify the connection listener of server-error") {
      proxy.stop
      val reply = serverListener !! new CountOneWayRequests("server-error")
      assertAtLeastOneReply(reply)
    }
    it("should notify the connection listener of client-connect") {
      val reply: Option[Any] = clientListener !! new CountOneWayRequests("client-connect")
      assertAtLeastOneReply(reply)
    }
    it("should notify the connection listener of client-disconnect") {
      val reply = clientListener !! new CountOneWayRequests("client-disconnect")
      assertAtLeastOneReply(reply)
    }
    it("should notify the connection listener of server-started") {
      val reply = serverListener !! new CountOneWayRequests("server-started")
      assertAtLeastOneReply(reply)
    }
    it("should notify the connection listener of server-client-disconnected") {
      val reply = serverListener !! new CountOneWayRequests("server-client-disconnected")
      RemoteClient.clientFor("localhost", 18000)
      assertAtLeastOneReply(reply)
    }
    it("should notify the connection listener of server-client-connected") {
      val reply = serverListener !! new CountOneWayRequests("server-client-connected")
      assertAtLeastOneReply(reply)
    }
    it("should notify the connection listener of server-shutdown") {
      server.shutdown
      val reply = serverListener !! new CountOneWayRequests("server-shutdown")
      assertAtLeastOneReply(reply)
    }
    it("should notify the connection listener of client-shutdown") {
      client.shutdown
      val reply = clientListener !! new CountOneWayRequests("client-shutdown")
      assertAtLeastOneReply(reply)
    }
  }

  def assertAtLeastOneReply(reply: Option[Any]) = {
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

class ConnectionListenerActor extends Actor {
  val map = new HashMap[String, Int]
  map += "error" -> 0
  map += "disconnect" -> 0
  map += "connect" -> 0
  def countEvent(event: String): Unit = {
    if (map.contains(event)) {
      map(event) = map(event) + 1
    } else {
      map += event -> 1
    }
  }

  def receive = {
    case RemoteClientError(cause, client: RemoteClient) => {
      log.info("listener: client error on %s:%s", client.hostname, client.port)
      countEvent("client-error")
    }
    case RemoteClientDisconnected(client: RemoteClient) => {
      log.info("listener: client disconnect on %s:%s", client.hostname, client.port)
      countEvent("client-disconnect")
    }
    case RemoteClientConnected(client: RemoteClient) => {
      log.info("listener: client connect on %s:%s", client.hostname, client.port)
      countEvent("client-connect")
    }
    case RemoteServerError(cause, server: RemoteServer) => {
      log.info("listener: server error.")
      countEvent("server-error")
    }
    case RemoteClientShutdown(client) => {
      log.info("listener: client shutdown.")
      countEvent("client-shutdown")
    }
    case RemoteServerShutdown(server) => {
      log.info("listener: server shutdown.")
      countEvent("server-shutdown")
    }
    case RemoteServerStarted(server) => {
      log.info("listener: server started.")
      countEvent("server-started")
    }
    case RemoteServerClientConnected(server, clientAddresss : Option[InetSocketAddress]) => {
      log.info("listener: client connected to server.")
      countEvent("server-client-connected")

    }
    case RemoteServerClientDisconnected(server, clientAddresss : Option[InetSocketAddress]) => {
      log.info("listener: client disconnected from server.")
      countEvent("server-client-disconnected")
    }
    case msg: CountOneWayRequests => {
      self.reply(new CountOneWayResponse(map.getOrElse(msg.data, 0)))
    }
  }
}