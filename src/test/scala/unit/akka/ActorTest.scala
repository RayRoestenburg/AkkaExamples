import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers

import se.scalablesolutions.akka.actor.{Actor}
import se.scalablesolutions.akka.actor.Actor._
/**A command message */
case class Command(name: String, data: String)
/** A message to get the amount of commands queued*/
case class CountCommandsQueued()
/** A message to execute all commands queued */
case class Execute()
/** A message reply on the CountCommandsQueued message */
case class CountResponse(count: Int)

/**
 * A Worker actor that receives Commands, queues the commands, and executes commands on the Execute message,
 * as example for some simple Actor testing
 */
class Worker extends Actor {
  var commands: List[Command] = Nil

  def receive = {
    case msg: Command => {
      commands = msg :: commands
    }
    case msg: Execute => {
      for (command <- commands) {
        commands = commands.dropRight(1)
      }
    }
    case msg: CountCommandsQueued => {
      self.reply(new CountResponse(commands.size))
    }
  }
}

/**
 * A Spec for the Worker Actor.
 */
class WorkerSpec extends Spec with MustMatchers {
  describe("A Worker") {

    describe("(when it queues commands)") {
      it("should have the correct number of commands queued") {

        val command = new Command("test", "data")
        val actorRef = actorOf(new Worker())
        actorRef.start
        var reply: CountResponse = (actorRef !! new CountCommandsQueued()).getOrElse(fail()).asInstanceOf[CountResponse]
        reply.count must be === 0

        actorRef ! command
        actorRef ! command
        actorRef ! command
        reply = (actorRef !! new CountCommandsQueued()).getOrElse(fail()).asInstanceOf[CountResponse]
        reply.count must be === 3
        actorRef.stop
      }
    }
    describe("(when it executes all queued commands)") {
      it("should have no commands queued after executing") {
        val command = new Command("test", "data")
        val actorRef = actorOf(new Worker())
        actorRef.start
        actorRef ! command
        actorRef ! command
        var reply: CountResponse = (actorRef !! new CountCommandsQueued()).getOrElse(fail()).asInstanceOf[CountResponse]
        reply.count must be === 2
        actorRef ! new Execute()
        reply = (actorRef !! new CountCommandsQueued()).getOrElse(fail()).asInstanceOf[CountResponse]
        reply.count must be === 0
        actorRef.stop
      }
    }
  }
}