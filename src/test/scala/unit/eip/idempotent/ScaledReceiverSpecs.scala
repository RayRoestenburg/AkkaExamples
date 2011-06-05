package unit.eip.idempotent

import eip.idempotent._
import tools.NetworkProxy
import org.scalatest.{Spec, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import akka.actor.{Actor, ActorRef}
import akka.actor.Actor._
import unit.test.proto.Commands.WorkerCommand
import java.util.concurrent.{TimeUnit, CyclicBarrier}
import eip.idempotent.IdempotentProtocol.{FrameRequestProtocol}
import java.io.File
import akka.remote.netty.{NettyRemoteSupport}
import akka.event.slf4j.Logging

class ScaledReceiverSpecs extends Spec with ShouldMatchers with BeforeAndAfterAll with Logging {
  def OptimizeLocal = false
  var optimizeLocal_? = remote.asInstanceOf[NettyRemoteSupport].optimizeLocalScoped_?
  val repeaterServerProxy = new NetworkProxy("localhost", 17000, 17095)
  val proxy = new NetworkProxy("localhost", 18000, 18094)
  val jgroupsConfigFile = new File("src/test/resources/jgroupsConfig.xml")
  val leftEnvelopes = new JGroupEnvelopes(jgroupsConfigFile, new MemEnvelopes(1, 10000, 10), "cluster-receivers", 10000)
  val BARRIER_TIMEOUT = 5000
  val leftIdempotentServer = new IdempotentServer(leftEnvelopes, 1000)

  val rightEnvelopes = new JGroupEnvelopes(jgroupsConfigFile, new MemEnvelopes(20000, 30000, 10), "cluster-receivers", 10000)
  val rightIdempotentServer = new IdempotentServer(rightEnvelopes, 1000)

  val repeatBuffer = new MemRepeatBuffer
  var repeaterClient = new RepeaterClient(new Address("localhost", 17000, "repeater"), repeatBuffer, 1000)
  val leftBarrier = new CyclicBarrier(2)
  val rightBarrier = new CyclicBarrier(2)
  var leftLocalActorRef: ActorRef = null
  var rightLocalActorRef: ActorRef = null
  var leftRemoteActorRef: ActorRef = null
  var rightRemoteActorRef: ActorRef = null
  val loadBalanceServer = new NettyRemoteSupport
  var repeaterRef: ActorRef = null

  override def beforeAll(configMap: Map[String, Any]) {
    if (!OptimizeLocal)
      remote.asInstanceOf[NettyRemoteSupport].optimizeLocal.set(false) //Can't run the test if we're eliminating all remote calls
    //returnAddress differs from start because the proxy is in between
    repeaterClient.start("localhost", 17095)
    repeaterServerProxy.start
    val leftModule = new NettyRemoteSupport
    leftIdempotentServer.remoteModule = leftModule
    val rightModule = new NettyRemoteSupport
    rightIdempotentServer.remoteModule = rightModule

    leftLocalActorRef = actorOf(new ConnTestActor(leftBarrier))
    rightLocalActorRef = actorOf(new ConnTestActor(rightBarrier))
    leftIdempotentServer.start("localhost", 18095)
    leftRemoteActorRef = leftIdempotentServer.register("remote-test-actor", leftLocalActorRef)

    rightIdempotentServer.start("localhost", 18096)
    rightRemoteActorRef = rightIdempotentServer.register("remote-test-actor", rightLocalActorRef)

    loadBalanceServer.start("localhost", 18094)
    proxy.start
    repeaterRef = repeaterClient.repeaterFor("remote-test-actor", "localhost", 18000);
    loadBalanceServer.register("remote-test-actor", actorOf(new RoundRobinActor(repeaterRef, List(leftRemoteActorRef, rightRemoteActorRef))))

  }

  override def afterAll(configMap: Map[String, Any]) {
    if (!OptimizeLocal)
      remote.asInstanceOf[NettyRemoteSupport].optimizeLocal.set(optimizeLocal_?) //Reset optimizelocal after all tests
    try {
      Thread.sleep(1000)
      proxy.stop
      repeaterServerProxy.stop
      leftIdempotentServer.shutdown
      rightIdempotentServer.shutdown
      loadBalanceServer.shutdown
      repeaterClient.close
    } catch {
      case e => ()
    } finally {
      remote.shutdownClientModule
    }
  }

  describe("Network load balanced idempotent receivers") {
    describe("when messages are sent to a load balancer") {
      it("should be received by one of the idempotent receivers for that frame") {

        for (i <- 1 to 9) {
          repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
          // all envelopes should be forwarded to one side (that owns the frame)
          leftBarrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
          leftBarrier.reset
          assertReply(leftLocalActorRef, i)
        }
        var frames = leftEnvelopes.getIncompleteFrames
        frames should have size (1)
        var incompleteFrame: Frame = null
        for (frame <- frames) {
          incompleteFrame = frame
          leftEnvelopes.getEnvelopeIds(frame.id) should have size (9)
          leftEnvelopes.isFrameComplete(frame.id) should be(false)
        }
        var otherFrames = rightEnvelopes.getIncompleteFrames
        otherFrames should have size (0)

        repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
        // all envelopes should be forwarded to one side (that owns the frame)
        leftBarrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
        leftBarrier.reset
        assertReply(leftLocalActorRef, 10)

        Thread.sleep(5000)
        // should now be complete
        otherFrames = rightEnvelopes.getIncompleteFrames
        otherFrames should have size (0)
        frames = leftEnvelopes.getIncompleteFrames
        frames should have size (0)
        leftEnvelopes.isFrameComplete(incompleteFrame.id) should be(true)
        leftEnvelopes.getEnvelopeIds(incompleteFrame.id) should have size (0)
        rightEnvelopes.getEnvelopeIds(incompleteFrame.id) should have size (0)
      }
      it("should receive another frame on the other idempotent receiver") {
        // should now switch to the other idempotent server
        for (i <- 1 to 9) {
          repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
          // all envelopes should be forwarded to one side (that owns the frame)
          rightBarrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
          rightBarrier.reset
          assertReply(rightLocalActorRef, i)

        }
        leftEnvelopes.getIncompleteFrames should have size (0)

        var otherFrames = rightEnvelopes.getIncompleteFrames
        otherFrames should have size (1)
        var frames = leftEnvelopes.getIncompleteFrames
        frames should have size (0)

        var incompleteFrame: Frame = null
        for (frame <- otherFrames) {
          incompleteFrame = frame
          rightEnvelopes.getEnvelopeIds(frame.id) should have size (9)
          rightEnvelopes.isFrameComplete(frame.id) should be(false)
        }
        repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
        // all envelopes should be forwarded to one side (that owns the frame)
        rightBarrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
        rightBarrier.reset
        assertReply(rightLocalActorRef, 10)
        Thread.sleep(5000)

        otherFrames = rightEnvelopes.getIncompleteFrames
        otherFrames should have size (0)
        frames = leftEnvelopes.getIncompleteFrames
        frames should have size (0)
        rightEnvelopes.isFrameComplete(incompleteFrame.id) should be(true)
        leftEnvelopes.getEnvelopeIds(incompleteFrame.id) should have size (0)
        rightEnvelopes.getEnvelopeIds(incompleteFrame.id) should have size (0)
        assertReply(rightLocalActorRef, 10)
        assertReply(leftLocalActorRef, 10)
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
