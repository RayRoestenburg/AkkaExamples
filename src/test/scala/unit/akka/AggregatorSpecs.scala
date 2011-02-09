import org.scalatest.matchers.MustMatchers
import org.scalatest.Spec
import akka.actor.Actor._
import akka.actor.{ActorRef, ActorRegistry, Actor}

/**
 * A Spec for the Aggregator
 */

class AggregatorSpecs extends Spec with MustMatchers {

  describe("An Aggregator") {

    describe("(when it receives FirstMessage and then SecondMessage)") {
      val firstRef = actorOf(new FirstMessageHandler())
      val secondRef = actorOf(new SecondMessageHandler())
      val receiveTestRef = actorOf(new ReceiveTestActor)
      val aggregator = actorOf(new Aggregator(receiveTestRef))
      firstRef.start
      secondRef.start
      aggregator.start
      receiveTestRef.start

      it("should send an AggregateMessage containing data of FirstMessage and SecondMessage to the passed in actor") {
        firstRef ! new FirstMessage("id-1", "name-1")
        Thread.sleep(200)
        secondRef ! new SecondMessage("data-1")
        Thread.sleep(1000)
        val reply : Option[Any] = receiveTestRef !! new RequestReceived
        if (reply.isDefined) {
          val tuple = reply.get.asInstanceOf[Tuple2[Boolean, AggregateMessage]]
          val aggregateMessageReceived = tuple._1
          val aggregateMessage = tuple._2

          aggregateMessageReceived must be === true
          aggregateMessage.id must be === "id-1"
          aggregateMessage.name must be === "name-1"
          aggregateMessage.data must be === "data-1"
          firstRef.stop
          secondRef.stop
          aggregator.stop
        }
      }
    }
  }
}

/**A message that is expected to arrive first*/

case class FirstMessage(id: String, name: String)

/**A message that is expected to arrive second*/

case class SecondMessage(data: String)

/**An aggregated message, from first and second */

case class AggregateMessage(id: String, name: String, data: String)

/**A command to get the last message*/

case class GiveMeLastMessage()

/**
 * An Aggregator actor that aggregates a first and second message type
 */

class Aggregator(pass: ActorRef) extends Actor {
  def receive = {
    case msg: SecondMessage => {
      println("Aggregator, my data is " + msg.data)
      val firstMessageHandler: ActorRef = registry.actorsFor(classOf[FirstMessageHandler]).head
      var reply: Option[Any] = firstMessageHandler !! new GiveMeLastMessage
      if (reply.isDefined) {
        val first: FirstMessage = reply.get.asInstanceOf[FirstMessage]
        println("Aggregator, my first message is " + first.id)
        val ag = new AggregateMessage(first.id, first.name, msg.data)
        pass ! ag
      }
    }
  }
}

/**
 * A Message Handler for the SecondMessage type
 */

class SecondMessageHandler extends Actor {
  def receive = {
    case msg: SecondMessage => {
      // do some processing
      println("Secondmessage, my data is " + msg.data)
      // then call the aggregator
      val aggregator: ActorRef = registry.actorsFor(classOf[Aggregator]).head
      aggregator ! msg
    }
  }
}

/**
 * A Message Handler for the FirstMessage type
 */

class FirstMessageHandler extends Actor {

  import self._

  var lastMessage: Option[FirstMessage] = None
  var lastRequestor: Option[Any] = None

  def receive = {
    case msg: FirstMessage => {
      // do some processing
      println("Firstmessage, my name is " + msg.name)

      lastMessage = Some(msg)
      if (lastRequestor != None) {
        val a = lastRequestor.asInstanceOf[ActorRef]
        a ! msg
        lastMessage = None
      }
    }
    case msg: GiveMeLastMessage => {
      if (!lastMessage.isDefined) {
        lastRequestor = senderFuture
      } else {
        reply(lastMessage.get)
      }
    }
  }
}

case class RequestReceived()

class ReceiveTestActor extends Actor {
  import self._
  var aggregateMessageReceived: Boolean = false
  var aggregateMessage: AggregateMessage = null
  def receive = {
    case msg: AggregateMessage => {
      aggregateMessageReceived = true
      aggregateMessage = msg
    }
    case msg: RequestReceived => {
      reply((aggregateMessageReceived, aggregateMessage))
    }
  }
}
