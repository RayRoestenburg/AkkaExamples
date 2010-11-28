package unit.eip.idempotent

import java.util.concurrent.CyclicBarrier
import collection.mutable.HashMap
import akka.actor.Actor
import akka.actor.Actor._
import unit.test.proto.Commands.WorkerCommand

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
    case msg: WorkerCommand => {
      if (map.contains(msg.getData)) {
        map(msg.getData) = map(msg.getData) + 1
      } else {
        map += msg.getData -> 1
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
