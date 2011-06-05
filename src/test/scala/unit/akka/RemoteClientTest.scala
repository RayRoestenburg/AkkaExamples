package unit.akka

import org.scalatest.{Spec, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import akka.actor.Actor._
import akka.actor.Actor
import akka.remote.netty.NettyRemoteSupport
import java.util.concurrent.{TimeUnit, CyclicBarrier}
import akka.dispatch.Future
import akka.event.slf4j.Logging

/**
 * Tests a remote actor interaction
 */
class RemoteClientTest extends Spec with ShouldMatchers with BeforeAndAfterAll with Logging {
  val barrier = new CyclicBarrier(2)

  def OptimizeLocal = false

  var optimizeLocal_? = remote.asInstanceOf[NettyRemoteSupport].optimizeLocalScoped_?


  override def beforeAll(configMap: Map[String, Any]) {
    if (!OptimizeLocal)
      remote.asInstanceOf[NettyRemoteSupport].optimizeLocal.set(false) //Can't run the test if we're eliminating all remote calls
    remote.start("localhost", 11000)
    remote.register("test", actorOf(new TestActor(barrier)))
  }

  override def afterAll(configMap: Map[String, Any]) {
    if (!OptimizeLocal)
      remote.asInstanceOf[NettyRemoteSupport].optimizeLocal.set(optimizeLocal_?) //Reset optimizelocal after all tests
    remote.shutdown
    remote.shutdownClientModule
    remote.shutdownServerModule
    registry.shutdownAll
  }

  describe("when sending to a remote actor") {
    it("should receive oneway with !") {
      val remoteActor = remote.actorFor("test", "localhost", 11000)
      remoteActor ! "hello"
      barrier.await(1, TimeUnit.SECONDS);
    }
    it("should send back a response on a !!") {
      val remoteActor = remote.actorFor("test", "localhost", 11000)
      val reply = remoteActor !! "hello"
      reply match {
        case Some(msg: String) => msg should be("hello")
        case None => fail("no reply")
      }
    }
    it("should send back a response on a !!!") {
      val remoteActor = remote.actorFor("test", "localhost", 11000)
      val future:Future[String] = remoteActor !!! "hello"
      future.await.result match {
        case Some(msg: String) => msg should be("hello")
        case None => fail("no reply")
      }
    }
  }
}

/**
 * Simple test actor. sets a barrier on !, sends the msg it receives on !! as the response.
 */
class TestActor(barrier: CyclicBarrier) extends Actor with Logging{
  def receive = {
    case msg: Any => {
      log.info("msg: %s", msg.toString)
      if (!self.reply_?(msg)) {
        //only await if it is a oneway
        barrier.await
      }
    }
  }
}