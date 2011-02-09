package unit.eip.idempotent

import org.scalatest.matchers.ShouldMatchers
import tools.NetworkProxy
import akka.actor.Actor._
import akka.actor.{ActorRef, Actor}
import java.util.concurrent.{TimeUnit, CyclicBarrier}
import eip.idempotent._
import org.scalatest.{BeforeAndAfterAll, Spec}
import akka.util.Logging
import unit.test.proto.Commands.WorkerCommand
import java.io.{InputStream, OutputStream}
import java.net.Socket
import akka.remote.netty.NettyRemoteSupport

class RepeaterAndReceiverSpecs extends Spec with ShouldMatchers with BeforeAndAfterAll with Logging {
  def OptimizeLocal = false
  var optimizeLocal_? = remote.asInstanceOf[NettyRemoteSupport].optimizeLocalScoped_?
  val repeaterServerProxy = new NetworkProxy("localhost", 17000, 18095)
  val proxy = new NetworkProxy("localhost", 18000, 18095)
  val envelopes = new MemEnvelopes(1, 1000, 10)
  val BARRIER_TIMEOUT = 5000
  val idempotentServer = new IdempotentServer(envelopes, 1000)
  val repeatBuffer = new MemRepeatBuffer
  var repeaterClient = new RepeaterClient(new Address("localhost", 17000, "repeater"), repeatBuffer, 1000)
  val barrier = new CyclicBarrier(2)
  var localActorRef: ActorRef = null
  // starting repeater on same port as server, since change in akka from RemoteServer to Actor.remote.
  //returnAddress differs from start because the proxy is in between
  repeaterClient.start("localhost", 18095)
  repeaterServerProxy.start

  override def beforeAll(configMap: Map[String, Any]) {
    if (!OptimizeLocal)
      remote.asInstanceOf[NettyRemoteSupport].optimizeLocal.set(false) //Can't run the test if we're eliminating all remote calls
    localActorRef = actorOf(new ConnTestActor(barrier))
    idempotentServer.start("localhost", 18095)
    idempotentServer.register("remote-test-actor", localActorRef)
    proxy.start

  }

  override def afterAll(configMap: Map[String, Any]) {
    if (!OptimizeLocal)
      remote.asInstanceOf[NettyRemoteSupport].optimizeLocal.set(optimizeLocal_?) //Reset optimizelocal after all tests
    try {
      Thread.sleep(1000)
      proxy.stop
      repeaterServerProxy.stop
      idempotentServer.shutdown
      repeaterClient.close
    } catch {
      case e => ()
    } finally {
      remote.shutdownClientModule
    }
  }

  describe("The Repeater") {
    describe("when a message is sent through the repeater") {
      it("should be received by the remote actor through the idempotent receiver") {
        envelopes.getIncompleteFrames should have size (0)
        val repeaterRef = repeaterClient.repeaterFor("remote-test-actor", "localhost", 18000);
        repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
        barrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
        // check that the remote server received the message
        assertReply(localActorRef, 1)
        envelopes.getIncompleteFrames should have size (1)
        val frames = repeatBuffer.getFrames("localhost", 18000)
        frames.size should equal(1)
        for (frame <- frames) {
          repeatBuffer.isFrameComplete(frame.id) should be(false)
          repeatBuffer.getEnvelopes(frame.id) should have size (1)
          for (envelope <- repeatBuffer.getEnvelopes(frame.id)) {
            envelope.getId should be(1)
            envelope.getFrameId should be(frame.id)
          }
        }
      }
    }
    describe("when the repeater sends a complete frame to the receiver") {
      it("should complete the frame") {
        val frames = repeatBuffer.getFrames("localhost", 18000)
        for (i <- 2 to 10) {
          barrier.reset
          val repeaterRef = repeaterClient.repeaterFor("remote-test-actor", "localhost", 18000);
          repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
          barrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
          // check that the remote server received the message
          assertReply(localActorRef, i)
        }
        Thread.sleep(1000)
        repeatBuffer.getFrames("localhost", 18000) should have size (0)
        envelopes.getIncompleteFrames should have size (0)
        // frame should be complete
        for (frame <- frames) {
          repeatBuffer.isFrameComplete(frame.id) should be(true)
          envelopes.getEnvelopeIds(frame.id) should have size (0)
          repeatBuffer.getEnvelopes(frame.id) should have size (0)
        }
      }
    }
    describe("when the repeater sends part of a frame to the receiver") {
      it("should contain the envelopes for that frame in the buffer") {
        var frames = repeatBuffer.getFrames("localhost", 18000)
        frames.size should equal(0)
        for (i <- 1 to 5) {
          barrier.reset
          val repeaterRef = repeaterClient.repeaterFor("remote-test-actor", "localhost", 18000);
          repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
          barrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
          // check that the remote server received the message
          assertReply(localActorRef, i + 10)
        }
        frames = repeatBuffer.getFrames("localhost", 18000)
        frames should have size (1)
        // frame should not be complete
        for (frame <- frames) {
          repeatBuffer.isFrameComplete(frame.id) should be(false)
          envelopes.getEnvelopeIds(frame.id) should have size (5)
          repeatBuffer.getEnvelopes(frame.id) should have size (5)
          for (envelope <- repeatBuffer.getEnvelopes(frame.id)) {
            envelope.getFrameId should be(frame.id)
          }
        }
      }
    }
    describe("when the idempotent receiver receives a duplicate envelope") {
      it("should ignore the envelope") {
        assertReply(localActorRef, 15)
        var frames = repeatBuffer.getFrames("localhost", 18000)
        frames should have size (1)
        for (frame <- frames) {
          val envelopesRepeater = repeatBuffer.getEnvelopes(frame.id)
          for (envelope <- envelopesRepeater) {
            val receiverActorRef = remote.actorFor("remote-test-actor", "localhost", 18000)
            receiverActorRef ! envelope
          }
          envelopes.getEnvelopeIds(frame.id) should have size (5)
        }
        assertReply(localActorRef, 15)
        envelopes.getIncompleteFrames should have size (1)
      }
    }
    describe("when a connection error occurs on the repeater side, and the connection is reconnected") {
      it("(repeater) should repeat the envelopes in the frame") {
        for (i <- 1 to 2) {
          barrier.reset
          val repeaterRef = repeaterClient.repeaterFor("remote-test-actor", "localhost", 18000);
          repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
          if (i == 1) {
            barrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
            assertReply(localActorRef, i + 15)
            // stop after first is received
            proxy.stop
          }
        }
        Thread.sleep(10000)
        // check that the message has not been received
        assertReply(localActorRef, 16)
        proxy.start
        // now wait until the second message comes in
        barrier.await
        assertReply(localActorRef, 17)
      }
      it("(receiver) should ignore duplicate envelopes") {
        var frames = repeatBuffer.getFrames("localhost", 18000)
        frames should have size (1)
        for (frame <- frames) {
          envelopes.getEnvelopeIds(frame.id) should have size (7)
        }
      }
    }
    describe("when a connection error occurs on the receiver side, and the connection is reconnected") {
      it("(receiver) should request repeat of the envelopes for incomplete frames") {
        for (i <- 1 to 2) {
          barrier.reset
          val repeaterRef = repeaterClient.repeaterFor("remote-test-actor", "localhost", 18000);
          repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
          if (i == 1) {
            barrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
            assertReply(localActorRef, i + 17)
            proxy.stop
          }
        }
        Thread.sleep(5000)
        repeaterServerProxy.stop
        Thread.sleep(5000)
        // check that the message has not been received
        assertReply(localActorRef, 18)
        proxy.start
        repeaterServerProxy.start
        // now wait until the second message comes in
        barrier.await
        assertReply(localActorRef, 19)
      }
      it("(repeater) should remove envelopes that were received by the receiver") {
        var previousFrames = repeatBuffer.getFrames("localhost", 18000)
        previousFrames should have size (1)
        for (frame <- previousFrames) {
          val envelopes = repeatBuffer.getEnvelopes(frame.id)
          // expecting repeat frames from server to have happened
          envelopes.size should be <= (9)
        }
        barrier.reset
        val repeaterRef = repeaterClient.repeaterFor("remote-test-actor", "localhost", 18000);
        repeaterRef ! WorkerCommand.newBuilder.setId(1L).setName("name-worker").setData("data-worker").build
        barrier.await(BARRIER_TIMEOUT, TimeUnit.MILLISECONDS)
        assertReply(localActorRef, 20)
        Thread.sleep(3000)
        // should now be complete
        var frames = repeatBuffer.getFrames("localhost", 18000)
        frames should have size (0)
        envelopes.getIncompleteFrames should have size (0)
        for (frame <- previousFrames) {
          repeatBuffer.isFrameComplete(frame.id) should be(true)
          envelopes.getEnvelopeIds(frame.id) should have size (0)
          repeatBuffer.getEnvelopes(frame.id) should have size (0)
        }
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