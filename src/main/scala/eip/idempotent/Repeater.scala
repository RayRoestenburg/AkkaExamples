package eip.idempotent

import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.Actor._
import java.util.concurrent.ConcurrentHashMap
import eip.idempotent.IdempotentProtocol._
import collection.JavaConversions._
import com.google.protobuf.Message
import akka.util.Logging
import collection.immutable.TreeSet
import java.net.InetSocketAddress
import akka.remoteinterface._

/**
 * Buffer for the Repeater
 */
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

/**
 * In memory implementation of the RepeatBuffer
 */
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

/**
 * Listens to errors on the connection from the Repeater side and handles requests from the IdempotentReceiver
 * for completing frames and repeat requests
 */
class RepeaterConnectionListener(repeatBuffer: RepeatBuffer) extends Actor {
  var repeat = false

  def repeatClient(remoteAddress: InetSocketAddress): Unit = {
    val frames = repeatBuffer.getFrames(remoteAddress.getHostName(), remoteAddress.getPort())

    if (!frames.isEmpty) {
      log.info("Repeating messages for client(%s:%d)", remoteAddress.getHostName(), remoteAddress.getPort())
    }
    for (frame <- frames) {
      val envelopes = repeatBuffer.getEnvelopes(frame.id)
      for (envelope <- envelopes) {
        val actorRef = remote.actorFor(frame.address.actor, frame.address.host, frame.address.port)
        actorRef ! envelope
      }
    }
  }

  def repeatFrame(frame: Frame): Unit = {
    val envelopes = repeatBuffer.getEnvelopes(frame.id)
    var sorted = new TreeSet()(Ordering.by((_: EnvelopeProtocol).getId).reverse)
    sorted = sorted ++ envelopes

    for (envelope <- sorted) {
      val actorRef = remote.actorFor(frame.address.actor, frame.address.host, frame.address.port)
      actorRef ! envelope
    }
  }

  def receive = {
    //remote client events
    case RemoteClientError(cause, client: RemoteClientModule, remoteAddress: InetSocketAddress) => {
      log.debug("Remote client(%s:%d) error", remoteAddress.getHostName, remoteAddress.getPort)
      repeat = true
    }
    case RemoteClientWriteFailed(request, error: Throwable, client: RemoteClientModule, address: InetSocketAddress) => {
      log.debug("Remote client(%s:%d) write failed", address.getHostName, address.getPort)
      repeat = true
    }

    case RemoteClientDisconnected(client: RemoteClientModule, remoteAddress: InetSocketAddress) => {
      log.debug("client(%s:%d) disconnected", remoteAddress.getHostName, remoteAddress.getPort)
      repeat = true
    }
    case RemoteClientConnected(client: RemoteClientModule, remoteAddress: InetSocketAddress) => {
      if (repeat) {
        log.debug("client(%s:%d) reconnected after error, startup or disconnect", remoteAddress.getHostName, remoteAddress.getPort)
        repeatClient(remoteAddress)
        repeat = false
      }
    }
    case RemoteClientStarted(client, address) => {
    }
    case RemoteClientShutdown(client: RemoteClientModule, remoteAddress: InetSocketAddress) => {
    }
    //remote server events
    case RemoteServerError(cause, server: RemoteServerModule) => {
      repeat = true
    }
    case RemoteServerShutdown(server: RemoteServerModule) => {
    }
    case RemoteServerStarted(server: RemoteServerModule) => {
      repeat = true
    }
    case RemoteServerClientClosed(server: RemoteServerModule, address: Option[InetSocketAddress]) => {
    }
    case RemoteServerClientConnected(server: RemoteServerModule, address: Option[InetSocketAddress]) => {
    }
    case RemoteServerClientDisconnected(server: RemoteServerModule, clientAddress: Option[InetSocketAddress]) => {
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
      spawn{
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

/**
 * Client to the idempotent remote actors. starts a remote server module for receiving messages from the Idempotent Receiver side
 */
class RepeaterClient(returnAddress: Address, repeatBuffer: RepeatBuffer, timeout: Int) extends Logging {
  private val repeaters = new JConcurrentMapWrapper(new ConcurrentHashMap[RepeaterKey, ActorRef])
  private val listenerRef = actorOf(new RepeaterConnectionListener(repeatBuffer))
  listenerRef.start
  remote.addListener(listenerRef)

  def start(host: String, port: Int, loader:ClassLoader) = {
    log.info("Starting server for repeater on %s, %d", host, port)
    if (!remote.isRunning) {
      remote.start(host, port,loader)
    }
    remote.register(returnAddress.actor, listenerRef)
  }

  def start(host: String, port: Int) = {
    log.info("Starting server for repeater on %s, %d", host, port)
    if (!remote.isRunning) {
      remote.start(host, port)
    }
    remote.register(returnAddress.actor, listenerRef)
  }

  def start = {
    log.info("Starting server for repeater on %s, %d", returnAddress.host, returnAddress.port)
    if (!remote.isRunning) {
      remote.start(returnAddress.host, returnAddress.port)
    }
    remote.register(returnAddress.actor, listenerRef)
  }

  def repeaterFor(remoteActor: String, host: String, port: Int): ActorRef = {
    val repeaterKey = new RepeaterKey(remoteActor, returnAddress, new Address(host, port, remoteActor))
    if (!repeaters.contains(repeaterKey)) {
      val address = new Address(host, port, remoteActor)
      log.debug("Creating new repeater for remote actor %s, returnAddress %s@%s:%d, address %s@%s:%d",
        remoteActor, returnAddress.actor, returnAddress.host, returnAddress.port, address.actor, address.host, address.port)
      remote.addListener(listenerRef)
      val actorRef = actorOf(new Repeater(repeatBuffer, returnAddress, address, timeout)).start
      repeaters.put(repeaterKey, actorRef)
    }
    repeaters.get(repeaterKey).get
  }

  def close = {
    remote.removeListener(listenerRef)
    try {
      for (repeaterKey <- repeaters.keys) {
        val address = repeaterKey.address
        remote.removeListener(listenerRef)
        remote.shutdownClientModule
      }
    } finally {
      remote.shutdown
    }
  }
}

/**
 * Repeater actor. requests frames, creates envelopes around messages and delegates these to a RemoteActorRef to the
 * IdempotentReceiver
 */
class Repeater(repeatBuffer: RepeatBuffer, returnAddress: Address, address: Address, timeout: Int) extends Actor {
  private var currentFrame: Frame = null

  def receive = {
    case msg: Message => {
      val envelope = createNewEnvelope()
      val envmsg = EnvelopeSerializer.serialize(envelope, msg)
      val actorRef = remote.actorFor(address.actor, address.host, address.port)
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
    var actorRef = remote.actorFor(address.actor, address.host, address.port)

    var success = false
    var frame: Frame = null
    while (!success) {
      try {
        if (actorRef.isShutdown) {
          actorRef = remote.actorFor(address.actor, address.host, address.port)
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
            // TODO how to do this
            remote.restartClientConnection(new InetSocketAddress(address.host, address.port))
          } catch {
            case e: Exception => {
              log.error("Error reconnecting to client before retry:%s", e.getMessage)
            }
          }
          Thread.sleep(timeout)
        }
      }
    }
    frame
  }
}
