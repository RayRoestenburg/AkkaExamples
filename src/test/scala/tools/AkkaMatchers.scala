package tools

import org.scalatest.matchers.{MatchResult, Matcher}
import akka.actor.{Actor, ActorRef}
import akka.dispatch.Future
import collection.mutable.ListBuffer

/**
 * Akka Matchers.
 * Enable you to write the following code:
 *
 * import
 * val end = actorOf(new End(timeout)).start
 * val actorRef = dock(actorOf(new MyActor(end)).start)
 * actorRef ! msg
 * end should receive "endresult"
 * end should waitfor "endresult"
 * sender !
 * sender should receive "response"
 * end should stack 6
 * actorRef should forward "test" to end
 * actorRef should forward 10 messages
 * actorRef should receive "test"
 * actorRef should
 *
 *
 */
/**trait AkkaMatchers {

  class ActorReceiveMatcher extends Matcher[ActorRef] {

    def apply(left: ActorRef) = {

      val failureMessageSuffix =
        " did not receive"

      val negatedFailureMessageSuffix =
        " received"

      MatchResult(
        left.isRunning,
        "The " + failureMessageSuffix,
        "The " + negatedFailureMessageSuffix,
        "the " + failureMessageSuffix,
        "the " + negatedFailureMessageSuffix
      )
    }
  }

  val receive = new ActorReceiveMatcher
}
class Sender {

}

class SenderActor {

}

class End {
  //actorRef to end so that the Matcher can work on it.
}
class EndActor extends Actor {
  def receive = {
  }
}
class Dock {

}
class DockActor(actorRef: ActorRef) extends Actor {
  val receivedList = ListBuffer[Any]()
  val sentList = ListBuffer[Any]()
  val responseList = ListBuffer[Any]()

  def receive = {
    case msg: Any => {
      receivedList.append(msg)
      // check if !!! or !! was used
      self.senderFuture match {
        case None => {
          actorRef ! msg
          actorRef.
          sentList.append(msg)
        }
        case Some(senderFuture) => {
          val future: Future[String] = actorRef !!! msg
          future.await.result match {
            case Some(response) => {
              responseList.append(response)
              senderFuture completeWithResult response
            }
            case None =>
          }
        }
      }
    }
  }
}
*/