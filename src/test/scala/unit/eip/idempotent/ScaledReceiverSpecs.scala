package unit.eip.idempotent

import akka.util.Logging
import eip.idempotent._
import tools.NetworkProxy
import org.scalatest.{Spec, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import akka.actor.{Actor, ActorRef}
import akka.actor.Actor._
import akka.remote.{RemoteServer, RemoteClient}
import unit.test.proto.Commands.WorkerCommand
import java.util.concurrent.{TimeUnit, CyclicBarrier}
import eip.idempotent.IdempotentProtocol.{FrameRequestProtocol, FrameResponseProtocol}
import java.io.File

class ScaledReceiverSpecs extends Spec with ShouldMatchers with BeforeAndAfterAll with Logging {
  val repeaterServerProxy = new NetworkProxy("localhost", 17000, 17095)
  val proxy = new NetworkProxy("localhost", 18000, 18094)
  val jgroupsConfigFile = new File("src/test/resources/jgroupsConfig.xml")
  val envelopes = new JGroupEnvelopes(jgroupsConfigFile, new MemEnvelopes(1,10000, 10), "cluster-receivers", 10000)
  val BARRIER_TIMEOUT = 5000
  val idempotentServer = new IdempotentServer(envelopes, 1000)

  val otherEnvelopes = new JGroupEnvelopes(jgroupsConfigFile, new MemEnvelopes(20000,30000, 10), "cluster-receivers", 10000)
  val otherIdempotentServer = new IdempotentServer(otherEnvelopes, 1000)

  val repeatBuffer = new MemRepeatBuffer
  var repeaterClient = new RepeaterClient(new Address("localhost", 17000, "repeater"), repeatBuffer, 1000)
  val barrier = new CyclicBarrier(2)
  val otherBarrier = new CyclicBarrier(2)
  var localActorRef: ActorRef = null
  var otherLocalActorRef: ActorRef = null
  var remoteActorRef: ActorRef = null
  var otherRemoteActorRef: ActorRef = null
  val loadBalanceServer = new RemoteServer
  var repeaterRef: ActorRef = null
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
    repeaterRef = repeaterClient.repeaterFor("remote-test-actor", "localhost", 18000);
    loadBalanceServer.register("remote-test-actor", actorOf(new RoundRobinActor(repeaterRef, List(remoteActorRef, otherRemoteActorRef))))

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
  describe("Network load balanced idempotent receivers") {
    describe("when messages are sent to a load balancer") {
      it("should be received by one of the idempotent receivers for that frame") {

        for (i <- 1 to 9) {
          repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
          // all envelopes should be forwarded to one side (that owns the frame)
          barrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
          barrier.reset
          assertReply(localActorRef,i)
        }
        var frames = envelopes.getIncompleteFrames
        frames should have size (1)
        var incompleteFrame: Frame = null
        for (frame <- frames) {
          incompleteFrame = frame
          envelopes.getEnvelopeIds(frame.id) should have size (9)
          envelopes.isFrameComplete(frame.id) should be(false)
        }
        var otherFrames = otherEnvelopes.getIncompleteFrames
        otherFrames should have size (0)

        repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
        // all envelopes should be forwarded to one side (that owns the frame)
        barrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
        barrier.reset
        assertReply(localActorRef,10)

        Thread.sleep(5000)
        // should now be complete
        otherFrames = otherEnvelopes.getIncompleteFrames
        otherFrames should have size (0)
        frames = envelopes.getIncompleteFrames
        frames should have size (0)
        envelopes.isFrameComplete(incompleteFrame.id) should be(true)
        envelopes.getEnvelopeIds(incompleteFrame.id) should have size (0)
        otherEnvelopes.getEnvelopeIds(incompleteFrame.id) should have size (0)
      }
      it("should receive another frame on the other idempotent receiver") {
        // should now switch to the other idempotent server
        for (i <- 1 to 9) {
          repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
          // all envelopes should be forwarded to one side (that owns the frame)
          otherBarrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
          otherBarrier.reset
          assertReply(otherLocalActorRef,i)

        }
        envelopes.getIncompleteFrames should have size(0)
        
        var otherFrames = otherEnvelopes.getIncompleteFrames
        otherFrames should have size (1)
        var frames = envelopes.getIncompleteFrames
        frames should have size (0)

        var incompleteFrame: Frame = null
        for (frame <- otherFrames) {
          incompleteFrame = frame
          otherEnvelopes.getEnvelopeIds(frame.id) should have size (9)
          otherEnvelopes.isFrameComplete(frame.id) should be(false)
        }
        repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
        // all envelopes should be forwarded to one side (that owns the frame)
        otherBarrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
        otherBarrier.reset
        assertReply(otherLocalActorRef,10)
        Thread.sleep(5000)

        otherFrames = otherEnvelopes.getIncompleteFrames
        otherFrames should have size (0)
        frames = envelopes.getIncompleteFrames
        frames should have size (0)
        otherEnvelopes.isFrameComplete(incompleteFrame.id) should be(true)
        envelopes.getEnvelopeIds(incompleteFrame.id) should have size (0)
        otherEnvelopes.getEnvelopeIds(incompleteFrame.id) should have size (0)
        assertReply(otherLocalActorRef,10)
        assertReply(localActorRef,10)
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

class RoundRobinActor(sender: ActorRef, actors: List[ActorRef]) extends Actor with Logging {
  var index = 0

  def nextActor: Unit = {
    index += 1
    if (index > actors.size - 1) {
      index = 0
    }
  }

  def receive = {
    case msg: FrameRequestProtocol => {
      log.info("sending frame request through to idempotent receiver %d", index)
      val response = actors(index) !! msg
      if (response.isDefined) {
        log.info("sending reply back through load balancer")
        self.reply(response.get)
      }
      nextActor
    }
    case msg: Any => {
      log.info("sending msg to idempotent receiver %d", index)
      actors(index) ! msg
      nextActor
    }
  }
}
