package eip.idempotent

import akka.actor.ActorRef
import akka.remote._
import akka.actor.Actor
import akka.actor.Actor._
import java.util.concurrent.ConcurrentHashMap
import eip.idempotent.IdempotentProtocol._
import collection.JavaConversions._
import com.google.protobuf.Message
import akka.util.Logging
import collection.immutable.TreeSet
import java.net.InetSocketAddress

trait RepeatBuffer {
  def addFrame(frame: Frame)

  def removeFrame(frameId: Int)

  def getFrames(hostname: String, port: Int): Iterable[Frame]

  def isFrameComplete(frameId: Int): Boolean

  def removeEnvelope(envelopeId: Int)

  def getNextEnvelopeId(frameId: Int): Int

  def getEnvelopes(frameId: Int): Iterable[EnvelopeProtocol]

  def addEnvelope(msg: EnvelopeProtocol)

  def sizeEnvelopes: Int

  def sizeFrames: Int
}

class MemRepeatBuffer extends RepeatBuffer {
  val messages = new JConcurrentMapWrapper(new ConcurrentHashMap[Int, EnvelopeProtocol])
  val frames = new JConcurrentMapWrapper(new ConcurrentHashMap[Int, Frame])
  val currentEnvelopeIds = new JConcurrentMapWrapper(new ConcurrentHashMap[Int, Int])

  def addFrame(frame: Frame) = {
    frames.put(frame.id, frame)
    currentEnvelopeIds.put(frame.id, 0)
  }

  def removeFrame(frameId: Int) = {
    frames.remove(frameId)
    currentEnvelopeIds.remove(frameId)
    val envelopePairs = messages.filter((pair: (Int, EnvelopeProtocol)) => pair._2.getFrameId == frameId)
    for (pair <- envelopePairs) {
      messages.remove(pair._1)
    }

  }

  def getNextEnvelopeId(frameId: Int): Int = {
    val envelopeId = currentEnvelopeIds.getOrElse(frameId, 0) + 1
    currentEnvelopeIds.put(frameId, envelopeId)
    envelopeId
  }

  def isFrameComplete(frameId: Int): Boolean = {
    val someFrame = frames.get(frameId)
    someFrame match {
      case Some(frame: Frame) => currentEnvelopeIds.getOrElse(frameId, 1) >= frame.size
      case None => true
    }
  }

  def getFrames(hostname: String, port: Int): Iterable[Frame] = {
    frames.filter((pair: (Int, Frame)) => pair._2.address.host == hostname && pair._2.address.port == port).values
  }

  def removeEnvelope(id: Int) = {
    messages.remove(id)
  }

  def getEnvelopes(frameId: Int): Iterable[EnvelopeProtocol] = {
    messages.filter((pair: (Int, EnvelopeProtocol)) => pair._2.getFrameId == frameId).values
  }

  def addEnvelope(msg: EnvelopeProtocol) = {
    messages.put(msg.getId, msg)
  }

  def sizeEnvelopes = messages.size

  def sizeFrames = frames.size
}

class RepeaterConnectionListener(repeatBuffer: RepeatBuffer) extends Actor {
  var repeat = false

  def repeatClient(client: RemoteClient): Unit = {
    val frames = repeatBuffer.getFrames(client.hostname, client.port)

    if (!frames.isEmpty) {
      log.info("Repeating messages for client(%s:%d)", client.hostname, client.port)
    }
    for (frame <- frames) {
      val envelopes = repeatBuffer.getEnvelopes(frame.id)
      for (envelope <- envelopes) {
        val actorRef = RemoteClient.actorFor(frame.address.actor, frame.address.host, frame.address.port)
        actorRef ! envelope
      }
    }
  }

  def repeatFrame(frame: Frame): Unit = {
    val envelopes = repeatBuffer.getEnvelopes(frame.id)
    var sorted = new TreeSet()(Ordering.by((_:EnvelopeProtocol).getId).reverse)
    sorted = sorted ++ envelopes

    for (envelope <- sorted) {
      val actorRef = RemoteClient.actorFor(frame.address.actor, frame.address.host, frame.address.port)
      actorRef ! envelope
    }
  }

  def receive = {
    //remote client events
    case RemoteClientError(cause, client: RemoteClient) => {
      log.debug("Remote client(%s:%d) error", client.hostname, client.port)
      repeat = true
    }
    case RemoteClientDisconnected(client: RemoteClient) => {
      log.debug("client(%s:%d) disconnected", client.hostname, client.port)
      repeat = true
    }
    case RemoteClientConnected(client: RemoteClient) => {
      if (repeat) {
        log.debug("client(%s:%d) reconnected after error, startup or disconnect", client.hostname, client.port)
        repeatClient(client)
        repeat = false
      }
    }
    case RemoteClientShutdown(client: RemoteClient) => {
    }
    //remote server events
    case RemoteServerError(cause, server: RemoteServer) => {
      repeat = true
    }
    case RemoteServerShutdown(server: RemoteServer) => {
    }
    case RemoteServerStarted(server: RemoteServer) => {
      repeat = true
    }
    case RemoteClientStarted(server: RemoteClient) => {
       repeat = true
    }

    case RemoteServerClientConnected(server: RemoteServer, address: Option[InetSocketAddress]) => {
    }
    case RemoteServerClientDisconnected(server: RemoteServer, clientAddress: Option[InetSocketAddress] ) => {
      repeat = true
    }

    // idempotent protocol
    case msg: RepeatFrameRequestProtocol => {
      log.debug("Received RepeatFrameRequest of frame %d,  ", msg.getFrame.getId)
      val frame = EnvelopeSerializer.fromProtocol(msg.getFrame)
      val ids = new JListWrapper(msg.getEnvelopeList)
      for (id <- ids) {
        repeatBuffer.removeEnvelope(id.intValue)
      }
      spawn {
        log.debug("Repeating frame %d on RepeatFrameRequest  ", msg.getFrame.getId)
        repeatFrame(frame)
      }
      self.reply(RepeatFrameResponseProtocol.newBuilder.setFrame(frame.toProtocol).build)
    }
    case msg: CompleteFrameRequestProtocol => {
      log.debug("Received CompleteFrameRequest for frame %d", msg.getFrame.getId)
      val frame = EnvelopeSerializer.fromProtocol(msg.getFrame)
      repeatBuffer.removeFrame(frame.id)
      self.reply(CompleteFrameResponseProtocol.newBuilder.setFrame(frame.toProtocol).build)
    }
  }
}
case class RepeaterKey(remoteActor: String, returnAddress: Address, address: Address)

class RepeaterClient(returnAddress: Address, repeatBuffer: RepeatBuffer, timeout: Int) extends Logging {
  private val repeaters = new JConcurrentMapWrapper(new ConcurrentHashMap[RepeaterKey, ActorRef])
  private val server = new RemoteServer
  private val listenerRef = actorOf(new RepeaterConnectionListener(repeatBuffer))
  listenerRef.start
  server.addListener(listenerRef)

  def start(host: String, port: Int) = {
    log.info("Starting server for repeater on %s, %d", host, port)
    server.start(host, port)
    server.register(returnAddress.actor, listenerRef)
  }

  def start = {
    log.info("Starting server for repeater on %s, %d", returnAddress.host, returnAddress.port)
    server.start(returnAddress.host, returnAddress.port)
    server.register(returnAddress.actor, listenerRef)
  }

  def repeaterFor(remoteActor: String, host: String, port: Int): ActorRef = {
    val repeaterKey = new RepeaterKey(remoteActor, returnAddress, new Address(host, port, remoteActor))
    if (!repeaters.contains(repeaterKey)) {
      val address = new Address(host, port, remoteActor)
      log.debug("Creating new repeater for remote actor %s, returnAddress %s@%s:%d, address %s@%s:%d",
        remoteActor, returnAddress.actor, returnAddress.host, returnAddress.port, address.actor, address.host, address.port)
      val client = RemoteClient.clientFor(address.host, address.port)
      client.addListener(listenerRef)
      val actorRef = actorOf(new Repeater(repeatBuffer, returnAddress, address, timeout)).start
      repeaters.put(repeaterKey, actorRef)
    }
    repeaters.get(repeaterKey).get
  }

  def close = {
    server.removeListener(listenerRef)
    try {
      for (repeaterKey <- repeaters.keys) {
        val address = repeaterKey.address
        val client = RemoteClient.clientFor(address.host, address.port)
        client.removeListener(listenerRef)
        client.shutdown
      }
    } finally {
      server.shutdown
    }
  }
}
class Repeater(repeatBuffer: RepeatBuffer, returnAddress: Address, address: Address, timeout: Int) extends Actor {
  private var currentFrame: Frame = null
  def receive = {
    case msg: Message => {
      val envelope = createNewEnvelope()
      val envmsg = EnvelopeSerializer.serialize(envelope, msg)
      val actorRef = RemoteClient.actorFor(address.actor, address.host, address.port)
      log.debug("Sending envelope %d,frame %d to %s, %s, %d", envelope.id, envelope.frameId, address.actor, address.host, address.port)
      actorRef ! envmsg
      repeatBuffer.addEnvelope(envmsg)
    }
  }

  def createNewEnvelope(): Envelope = {
    if (currentFrame == null || repeatBuffer.isFrameComplete(currentFrame.id)) {
      currentFrame = requestNewFrame()
    }
    new Envelope(repeatBuffer.getNextEnvelopeId(currentFrame.id), currentFrame.id)
  }

  def requestNewFrame(): Frame = {
    log.debug("Requesting new frame from actor %s, host %s, port %d", address.actor, address.host, address.port)
    var actorRef = RemoteClient.actorFor(address.actor, address.host, address.port)

    var success = false
    var frame: Frame = null
    while (!success) {
      try {
        if(actorRef.isShutdown){
          actorRef = RemoteClient.actorFor(address.actor, address.host, address.port)
        }
        val someResponse = actorRef !! FrameRequestProtocol.newBuilder.setAddress(address.toProtocol).setReturnAddress(returnAddress.toProtocol).build
        someResponse match {
          case Some(response: FrameResponseProtocol) => {
            success = true
            frame = Frame.fromProtocol(response.getFrame)
            repeatBuffer.addFrame(frame)
          }
          case None => {
            log.error("Timeout requesting new frame(%s, %s,%d). retrying.", address.actor, address.host, address.port)
            Thread.sleep(timeout)
          }
        }
      } catch {
        case e: Exception => {
          log.error("Error requesting new frame(%s, %s,%d). retrying.", address.actor, address.host, address.port)
          try {
            RemoteClient.clientFor( address.host, address.port).connect
          } catch {
            case e:Exception => {
              log.error("Error reconnecting to client before retry:%s",e.getMessage)
            }
          }
          Thread.sleep(timeout)
        }
      }
    }
    frame
  }
}
