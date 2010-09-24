package unit.eip.idempotent

import se.scalablesolutions.akka.util.Logging
import eip.idempotent._
import tools.NetworkProxy
import org.scalatest.{Spec, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import se.scalablesolutions.akka.actor.{Actor, ActorRef}
import se.scalablesolutions.akka.actor.Actor._
import se.scalablesolutions.akka.remote.{RemoteServer, RemoteClient}
import unit.test.proto.Commands.WorkerCommand
import eip.idempotent.IdempotentProtocol.FrameResponseProtocol
import java.util.concurrent.{TimeUnit, CyclicBarrier}

class ScaledReceiverSpecs extends Spec with ShouldMatchers with BeforeAndAfterAll with Logging {
  val repeaterServerProxy = new NetworkProxy("localhost", 17000, 17095)
  val proxy = new NetworkProxy("localhost", 18000, 18094)
  val envelopes = new JGroupEnvelopes(null, new MemEnvelopes(10), "cluster-receivers", 10000)
  val BARRIER_TIMEOUT = 5000
  val idempotentServer = new IdempotentServer(envelopes, 1000)

  val otherEnvelopes = new JGroupEnvelopes(null, new MemEnvelopes(10), "cluster-receivers", 10000)
  val otherIdempotentServer = new IdempotentServer(envelopes, 1000)

  val repeatBuffer = new MemRepeatBuffer
  var repeaterClient = new RepeaterClient(new Address("localhost", 17000, "repeater"), repeatBuffer, 1000)
  val barrier = new CyclicBarrier(2)
  val otherBarrier = new CyclicBarrier(2)
  var localActorRef: ActorRef = null
  var otherLocalActorRef: ActorRef = null
  var remoteActorRef: ActorRef = null
  var otherRemoteActorRef: ActorRef = null
  val loadBalanceServer = new RemoteServer
  var repeaterRef:ActorRef = null
  //returnAddress differs from start because the proxy is in between
  repeaterClient.start("localhost", 17095)
  repeaterServerProxy.start

  override def beforeAll(configMap: Map[String, Any]) {
    localActorRef = actorOf(new ConnTestActor(barrier))
    otherLocalActorRef = actorOf(new ConnTestActor(otherBarrier))
    idempotentServer.start("localhost", 18095)
    remoteActorRef = idempotentServer.register("remote-test-actor", localActorRef)
    otherIdempotentServer.start("localhost", 18096)
    otherRemoteActorRef = otherIdempotentServer.register("remote-test-actor", otherLocalActorRef)
    loadBalanceServer.start("localhost", 18094)
    proxy.start
    repeaterRef = repeaterClient.repeaterFor("load-balancer", "localhost", 18000);
    loadBalanceServer.register("load-balancer", actorOf(new RoundRobinActor(repeaterRef, List(remoteActorRef, otherRemoteActorRef))))

  }

  override def afterAll(configMap: Map[String, Any]) {
    try {
      Thread.sleep(1000)
      proxy.stop
      repeaterServerProxy.stop
      idempotentServer.shutdown
      otherIdempotentServer.shutdown
      loadBalanceServer.shutdown
      repeaterClient.close
    } catch {
      case e => ()
    } finally {
      RemoteClient.shutdownAll
    }
  }
  describe("The Repeater") {
    describe("when a message is sent to a load balancer") {
      it("should be received by one of the idempotent receivers") {

        for (i <- 1 to 10) {
          repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
          if(i% 2 == 0){
            otherBarrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
          } else {
            barrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
          }
        }
        Thread.sleep(10000)
      }
    }
  }

  def assertReply(actorRef: ActorRef, countExpected: Int) = {
    var reply: Option[Any] = actorRef !! new CountOneWayRequests("data-worker")
    reply match {
      case Some(response: CountOneWayResponse) => {
        response.count should equal(countExpected)
        log.info("received reply correctly")
      }
      case None => fail("no reply")
    }
  }
}

class RoundRobinActor(sender:ActorRef, actors: List[ActorRef]) extends Actor {
  var index = 0
  def receive = {
    case msg: AnyRef => {
      if(self.sender.isDefined){
        val response = actors(index) !! msg
        if(response.isDefined){
          self.reply(response.get)
        }
      } else {
        actors(index) ! msg
      }
      index += 1
      if (index > actors.size - 1) {
        index = 0
      }
    }
  }
}