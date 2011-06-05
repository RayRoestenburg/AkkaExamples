package supervision

import akka.actor.Actor
import java.io.{Closeable, Flushable, IOException, Writer}

/**
 * Idea for supervision example
 * publishes to a Writer.
 * Temporary lifecycle needed, on failure will be stopped.
 * TODO how to handle blocking Actor -> could use futures inside actor? or check mailboxsize and kill from supervisor
 * TODO how
 */
class Publisher(out:Writer) extends Actor {
  private var closed = false


  def receive = {
    case msg: Writer =>
    case msg: Header => out.write("{%s:%d, elements:[".format(msg.name, msg.version))
    case msg: Element => out.write("{%s:%s}".format(msg.name, msg.value))
    case msg: Footer => out.write("]}")
    case msg: Close => {
      close
      self.stop
    }
    case msg: Cancel => close
  }

  override def postStop() {
    close
  }

  private def close = {
    if(!closed) {
      try {
      out.close()
      } catch {
        case e:Exception => // dont care
      }
    }
    closed = true
  }
}

case class Close()
case class Cancel()
case class Header(name:String, version:Int)
case class Footer()
case class Element(name:String, value:String)