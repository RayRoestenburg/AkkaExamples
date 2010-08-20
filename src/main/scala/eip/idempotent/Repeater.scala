package eip.idempotent

import se.scalablesolutions.akka.actor.ActorRef
import se.scalablesolutions.akka.remote._
import se.scalablesolutions.akka.actor.Actor
import se.scalablesolutions.akka.actor.Actor._
import se.scalablesolutions.akka.stm.local._
import java.util.concurrent.ConcurrentHashMap
import se.scalablesolutions.akka.persistence.redis.RedisStorage
import org.codehaus.aspectwerkz.proxy.Uuid
import eip.idempotent.IdempotentProtocol.EnvelopeProtocol
import eip.idempotent._
import com.google.protobuf.Message

object Repeater {
  val server = new RemoteServer
  var host: String = "localhost"
  var port: Int = 18094

  // keep repeaters per host and port and remoteActorName
  // keep a RemoteServer for receiving acknowledgements
  def repeater(remoteActorName: String, remoteHost: String, remotePort: Int): ActorRef = {
    if (!server.isRunning) {
      server.start(host, port)
    }

    val client = RemoteClient.clientFor(remoteHost, remotePort)
    val actorRef = RemoteClient.actorFor(remoteActorName, remoteHost, remotePort)
    val repeaterActor = actorOf(new Repeater(Set(actorRef)))
    repeaterActor.start
    server.register("repeater", repeaterActor)
    client.registerListener(repeaterActor)
    repeaterActor
  }

  def tracked(msg: Message): EnvelopeProtocol = {
    val envelope = new Envelope(Uuid.newUuid, System.currentTimeMillis, new Sender(host, port, "repeater"))
    EnvelopeSerializer.serialize(envelope, msg)
  }
}

trait RepeatBuffer {
  def remove(id: Long)

  def foreach(f: (EnvelopeProtocol) => EnvelopeProtocol): Unit

  def add(msg: EnvelopeProtocol)
}

class RedisRepeatBuffer(storageKey: String) extends RepeatBuffer {
  private lazy val storage = atomic {RedisStorage.getMap(storageKey)}

  def remove(id: Long) = atomic {
    storage.remove(id.toString.getBytes)
    storage.commit
  }

  def foreach(f: (EnvelopeProtocol) => EnvelopeProtocol): Unit = {
    val fBytes: Array[Byte] => EnvelopeProtocol = {bytes => EnvelopeProtocol.parseFrom(bytes)}
    storage.values.foreach[EnvelopeProtocol] {e => fBytes(e)}
  }

  def add(msg: EnvelopeProtocol) = atomic {
    storage.put(msg.getId.toString.getBytes, msg.toByteArray)
    storage.commit
  }

  def size = storage.size
}
class MemRepeatBuffer extends RepeatBuffer {
  val messages = new ConcurrentHashMap[Long, EnvelopeProtocol]

  def remove(id: Long) = {
    messages.remove(id)
  }

  def foreach(f: (EnvelopeProtocol) => EnvelopeProtocol): Unit = {
    //TODO implement
    // messages.values.foreach[EnvelopeProtocol] { f }
  }

  def add(msg: EnvelopeProtocol) = {
    messages.put(msg.getId, msg)
  }

  def size = messages.size
}

/**
 * Registered as listener for remote errors, and used as forwarder of tracked messages
 */
class Repeater(actors: Set[ActorRef]) extends Actor {
  val repeatBuffer = new RedisRepeatBuffer("test")
  var repeat = false
  // keep a ref to the last X messages
  def receive = {
    case RemoteClientError(cause, hostname, port) => {
      log.info("LOOP:LISTEN Remote client error")
      repeat = true
    }
    case RemoteClientDisconnected(hostname, port) => {
      log.info("LOOP:LISTEN Remote client disconnected")
      // set state to start repeating at Connected
      repeat = true
    }
    case RemoteClientConnected(hostname, port) => {
      log.info("LOOP:LISTEN Remote client connected, start repeating")
      // start repeating  if connected after disconnected, or connected after remote client error
      if (repeat) {
        if (repeatBuffer.size > 0) {
          val send: EnvelopeProtocol => EnvelopeProtocol = (msg: EnvelopeProtocol) => {
            for (actor <- actors) {
              log.info("LOOP: repeating " + msg.getId + " from  buffer")
              actor ! msg

            }
            msg
          }
          // spawn an actor to repeat until the buffersize = 0, which
          // happens when the idempotentReceiver sends acknowledgements back
          spawn {
            while (repeatBuffer.size > 0) {
              repeatBuffer foreach send
              // TODO configurable
              Thread.sleep(2000)
            }
          }
        } else {
          repeat = false
        }
      }
    }
    case msg: EnvelopeProtocol => {
      for (actor <- actors) {
        log.info("LOOP: sending " + msg.getId + " from repeater")
        actor ! msg
      }
      log.info("LOOP: adding " + msg.getId + " to buffer")
      repeatBuffer.add(msg)
    }
    case msg: Acknowledgements => {
      for (id <- msg.ids) {
        log.info("LOOP: ack received, removing " + id + " from buffer")
        repeatBuffer.remove(id)
      }
    }
    case msg: BufferSizeRequest => {
      self.reply(new BufferSizeResponse(repeatBuffer.size))
    }
  }
}