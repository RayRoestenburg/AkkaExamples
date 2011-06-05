package eip.idempotent

import java.lang.String
import org.jgroups.{Message, ReceiverAdapter, JChannel}
import akka.stm._
import collection.JavaConversions.JConcurrentMapWrapper
import eip.idempotent.IdempotentProtocol.EnvelopeProtocol
import collection.mutable.Queue
import java.util.concurrent.ConcurrentHashMap
import java.io.{ObjectOutput, ObjectInput, Externalizable, File}
import akka.event.slf4j.Logging
/**
 * Keeps track of Envelopes on the Idempotent Receiver side
 */
trait Envelopes {
  /**
   * Clears the envelopes
   */
  def clear

  /**
   * returns the envelope for the id passed
   */
  def get(frameId: Int, id: Int, payload: Any): Option[Envelope]

  /**
   * stores the envelope, returns true if the frame is complete
   */
  def put(envelope: Envelope): Boolean

  /**
   * Gets the next frame
   */
  def nextFrame(returnAddress: Address, address: Address): Frame

  /**
   * removes all envelopes in the frame, returns true if the Frame could be removed.
   * Will only remove if there are as many envelopes for the frame as the frame size (frame complete) 
   */
  def removeFrame(frameId: Int): Boolean

  /**
   * gets the frame
   */
  def getFrame(frameId: Int): Option[Frame]

  /**
   * returns if the frame is complete
   */
  def isFrameComplete(frameId: Int): Boolean

  /**
   * gets the envelope ids for the frame.
   */
  def getEnvelopeIds(frameId: Int): Set[Int]

  /**
   * gets the incomplete Frames
   */
  def getIncompleteFrames(): Set[Frame]

  /**
   * size of envelopes
   */
  def size: Int

  /**
   * Closes the envelopes store
   */
  def close: Unit = {}

  /**
   * Pass reference to idempotent server to envelopes
   */
  def setServer(server: IdempotentServer)

}

/**
 * In Memory Envelopes
 */
class MemEnvelopes(frameIdStart: Int, frameIdEnd: Int, frameSize: Int) extends Envelopes with Logging {
  private val envelopes = new JConcurrentMapWrapper(new ConcurrentHashMap[(Int, Int), Envelope])
  private val frameCounts = new JConcurrentMapWrapper(new ConcurrentHashMap[Int, Int])
  private val frames = new JConcurrentMapWrapper(new ConcurrentHashMap[Int, Frame])
  private var idempotentServer: IdempotentServer = null

  //TODO later extract 'next Frame' into a strategy
  val ref = Ref(frameIdStart-1)

  def nextFrameId = atomic {
    var next = ref alter (_ + 1)
    if (next > frameIdEnd) {
      next = ref alter ( _ => frameIdStart)
    }
    next
  }

  def nextFrame(returnAddress: Address, address: Address): Frame = {
    val frame = new Frame(nextFrameId.intValue, frameSize, returnAddress, address)
    addFrame(frame)
    frame
  }

  def addFrame(frame: Frame) = {
    frames += frame.id -> frame
  }

  def clear = {
    envelopes.clear
    frameCounts.clear
    frames.clear
  }

  def get(frameId: Int, id: Int, payload: Any): Option[Envelope] = {
    envelopes.get((frameId, id))
  }


  def isFrameComplete(frameId: Int): Boolean = {
    frames.get(frameId).exists(frame => frameCounts.getOrElse(frameId, 0) >= frame.size)
  }

  def getEnvelopeIds(frameId: Int): Set[Int] = {
    envelopes.filter(pair => pair._2.frameId == frameId).values.map(envelope => envelope.id).toSet
  }

  def getIncompleteFrames(): Set[Frame] = {
    frames.filter(pair => frameCounts.getOrElse(pair._1, 0) < pair._2.size).values.toSet
  }

  def getFrame(frameId: Int): Option[Frame] = {
    frames.get(frameId)
  }


  def put(envelope: Envelope): Boolean = {
    var complete = false
    if (!envelopes.contains((envelope.frameId, envelope.id))) {
      envelopes += (envelope.frameId, envelope.id) -> envelope
      val someCount = frameCounts.get(envelope.frameId)
      someCount match {
        case Some(count) => {
          var updated = count + 1
          frameCounts += envelope.frameId -> updated
          if (frameSize <= updated) complete = true
        }
        case None => {
          frameCounts += envelope.frameId -> 1
        }
      }
    }
    complete
  }

  def removeFrame(frameId: Int): Boolean = {
    val someCount = frameCounts.get(frameId)
    var removed = false;
    someCount match {
      case Some(count) => {
        if (frameSize == count) {
          envelopes.retain((key, value) => value.frameId != frameId)
          removed = true;
        } else {
          removed = false
        }
      }
      case None => {
      }
    }
    removed
  }

  def size: Int = {
    envelopes.size
  }

  def setServer(server: IdempotentServer) = {
    idempotentServer = server
  }
}

/**
 * JGroupEnvelopes. Instead of distributing state,
 * the servers "own" frames that they have created, frames are in this way partitioned over the servers by frame Id.
 * if the server receives a frame that it does not own, it forwards it to all the other servers.
 * If a server has crashed, JGroups should retransmit until it is up again.
 */
class JGroupEnvelopes(configFile: File, source: Envelopes, cluster: String, timeoutConnectCluster: Int) extends ReceiverAdapter with Envelopes with Logging {
  private var idempotentServer: IdempotentServer = null

  val SIZE_COMPLETED_FRAMES_HISTORY = 100;
  val lock = new Object()
  val recentlyCompletedFrameIds = new Queue[Int]
  val channel = new JChannel(configFile)
  channel.setReceiver(this);
  channel.connect(cluster);

  if (channel.getState(null, timeoutConnectCluster)) {
    log.info("Creating the cluster %s", cluster)
  } else {
    log.info("Joining the cluster %s", cluster)
  }
  def setServer(server: IdempotentServer) = {
    if(server == null) throw new Exception("server cannot be null at setServer")
    idempotentServer = server
  }

  override def receive(msg: Message): Unit = {
    if(idempotentServer == null) {
      log.error("Idempotent server has to be set through setServer call")
      throw new Exception("idempotentServer has to be set")
    }
    log.debug("receiving message on jgroups, %s, from %s", channel.getAddress.toString, msg.getSrc.toString)
    if (!msg.getSrc.equals(channel.getAddress)) {
      val exEnv = msg.getObject.asInstanceOf[ExternalizableEnvelope]
      val envelopeProtocol = exEnv.getEnvelopeProtocol
      val protomsg: (Envelope, Any) = EnvelopeSerializer.deserialize(envelopeProtocol)
      val envelope = protomsg._1
      val payload = protomsg._2
      val frameId = envelope.frameId
      source.getFrame(frameId) match {
        case Some(frame) => {
          log.debug("Server owns envelope %d for frame %d received through JGroups", envelope.id, frameId)
          // this server owns the frame, so send the Envelope to the receiver (through RemoteClient (should be on same server) cause it is easy)
          val someActorRef = idempotentServer.idempotentActorRef(frame.address.actor)
          someActorRef match {
            case Some(actorRef) => actorRef ! envelopeProtocol
            case None => log.error("Configuration error, could not find idempotent actor %s, envelope %d of frame %d will be lost".format( frame.address.actor, envelope.id, frameId))
          }
        }
        case None => log.debug("Ignoring envelope %d, received through JGroups, not owning frame %d", envelope.id, frameId)
      }
    } else {
      log.debug("sending to self on jgroups")
    }
  }

  override def close = channel.close


  def clear = {
    source.clear
  }

  def get(frameId: Int, id: Int, payload: Any): Option[Envelope] = {
    source.getFrame(frameId) match {
      case Some(frame) => {
        log.debug("owning frame %d", frameId)
        source.get(frameId, id, payload) //this Envelopes owns the frame
      }
      case None => {
        log.debug("not owning frame %d", frameId)
        if (!recentlyCompletedFrameIds.contains(frameId)) {
          log.debug("not recently completed frame %d", frameId)
          //send to all others in cluster if the frame is not owned by this server,
          val protocol = EnvelopeSerializer.serialize(new Envelope(id, frameId), payload.asInstanceOf[com.google.protobuf.Message])
          channel.send(null, channel.getAddress, new ExternalizableEnvelope(protocol))
          log.debug("sent envelope to others %d, %d", frameId, id)
        }
        Some(new Envelope(frameId, id))
      }
    }
  }

  def nextFrame(returnAddress: Address, address: Address): Frame = {
    source.nextFrame(returnAddress, address)
  }

  def isFrameComplete(frameId: Int): Boolean = {
    source.isFrameComplete(frameId)
  }

  def getEnvelopeIds(frameId: Int): Set[Int] = {
    source.getEnvelopeIds(frameId)
  }

  def getIncompleteFrames(): Set[Frame] = {
    source.getIncompleteFrames
  }

  def getFrame(frameId: Int): Option[Frame] = {
    source.getFrame(frameId)
  }


  def put(envelope: Envelope): Boolean = {
    val complete = source.put(envelope)
    if (complete) {
      lock.synchronized {
        recentlyCompletedFrameIds.enqueue(envelope.frameId)
        if (recentlyCompletedFrameIds.size > SIZE_COMPLETED_FRAMES_HISTORY) {
          log.debug("limiting completed framed ids: %d", recentlyCompletedFrameIds.size)

          recentlyCompletedFrameIds.dequeue
          log.debug("limited completed framed ids: %d", recentlyCompletedFrameIds.size)
        }
      }
    }
    complete
  }

  def removeFrame(frameId: Int): Boolean = {
    source.removeFrame(frameId)
  }

  def size: Int = {
    source.size
  }
}

class ExternalizableEnvelope(message: EnvelopeProtocol) extends Externalizable {
  var msg = message

  def this() {
    this (EnvelopeProtocol.newBuilder.setId(0).setFrameId(0).build)
  }

  def readExternal(p1: ObjectInput) = {
    val arraySize = p1.readInt
    val array = new Array[Byte](arraySize)
    p1.read(array)
    msg = EnvelopeProtocol.parseFrom(array)
  }

  def writeExternal(p1: ObjectOutput) = {
    val bytes = msg.toByteArray
    p1.writeInt(bytes.size)
    p1.write(bytes)
  }

  def getEnvelopeProtocol(): EnvelopeProtocol = {
    msg
  }
}
