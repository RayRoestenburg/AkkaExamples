package eip.idempotent

import java.lang.String
import org.jgroups.{Message, ReceiverAdapter, JChannel}
import se.scalablesolutions.akka.stm.local._
import collection.JavaConversions.JConcurrentMapWrapper
import eip.idempotent.IdempotentProtocol.EnvelopeProtocol
import se.scalablesolutions.akka.remote.RemoteClient
import collection.mutable.Queue
import java.util.concurrent.ConcurrentHashMap
import java.io.{ObjectOutput, ObjectInput, Externalizable, File}

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
}

/**
 * In Memory Envelopes
 */
class MemEnvelopes(frameSize: Int) extends Envelopes {
  private val envelopes = new JConcurrentMapWrapper(new ConcurrentHashMap[(Int, Int), Envelope])
  private val frameCounts = new JConcurrentMapWrapper(new ConcurrentHashMap[Int, Int])
  private val frames = new JConcurrentMapWrapper(new ConcurrentHashMap[Int, Frame])

  val ref = Ref(0)

  def nextFrameId = atomic {
    ref alter (_ + 1)
  }


  def clear = {
    envelopes.clear
    frameCounts.clear
    frames.clear
  }

  def get(frameId: Int, id: Int, payload: Any): Option[Envelope] = {
    envelopes.get((frameId, id))
  }

  def nextFrame(returnAddress: Address, address: Address): Frame = {
    val frame = new Frame(nextFrameId.intValue, frameSize, returnAddress, address)
    frames += frame.id -> frame
    frameCounts
    frame
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
}

/**
 * TODO implement hooks in JGroups to see crashes. maybe implement storage to select specific owner of frame, and takeover if not there 
 * (need to check if owner gets message after restart)
 * JGroupEnvelopes. Instead of distributing state,
 * the servers "own" frames that they have created, frames are in this way partitioned over the servers by frame Id.
 * if the server receives a frame that it does not own, it forwards it to all the other servers.
 */
class JGroupEnvelopes(configFile: File, source: Envelopes, cluster: String, timeoutConnectCluster:Int) extends ReceiverAdapter with Envelopes {
  val SIZE_COMPLETED_FRAMES_HISTORY = 100;
  val lock = new Object()
  val recentlyCompletedFrameIds = new Queue[Int]
  val channel = new JChannel()// TODO configFile)
  channel.setReceiver(this);
  channel.connect(cluster);
  
  if (channel.getState(null, timeoutConnectCluster)) {
    log.info("Creating the cluster %s", cluster)
  } else {
    log.info("Joining the cluster %s", cluster)
  }

  override def receive(msg: Message): Unit = {
    if (!msg.getSrc.equals(channel.getAddress)) {
      val exEnv= msg.getObject.asInstanceOf[ExternalizableEnvelope]
      val envelopeProtocol = exEnv.getEnvelopeProtocol
      val protomsg: (Envelope, Any) = EnvelopeSerializer.deserialize(envelopeProtocol)
      val envelope = protomsg._1
      val payload = protomsg._2
      val frameId = envelope.frameId
      source.getFrame(frameId) match {
        case Some(frame) => {
          // this server owns the frame, so send the Envelope to the receiver (through RemoteClient cause it is easy)
          val actorRef = RemoteClient.actorFor(frame.address.actor, frame.address.host, frame.address.port)
        }
        case None => log.debug("Ignoring envelope %d, received through JGroups, not owning frame %d", envelope.id, frameId)
      }
    }
  }

  override def close = channel.close


  def clear = {
    source.clear
  }

  def get(frameId: Int, id: Int, payload: Any): Option[Envelope] = {
    source.getFrame(frameId) match {
      case Some(frame) => source.get(frameId, id,payload) //this Envelopes owns the frame
      case None => {
        if (!recentlyCompletedFrameIds.contains(frameId)) {
          //send to all others in cluster if the frame is not owned by this server,
          val protocol = EnvelopeSerializer.serialize(new Envelope(id, frameId), payload.asInstanceOf[com.google.protobuf.Message])
          channel.send(null, channel.getAddress, new ExternalizableEnvelope(protocol))
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
          recentlyCompletedFrameIds.dequeue
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

class ExternalizableEnvelope(message:EnvelopeProtocol) extends Externalizable {
  var msg = message
  
  def this() {
    this(EnvelopeProtocol.newBuilder.setId(0).setFrameId(0).build)
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
  def getEnvelopeProtocol() : EnvelopeProtocol = {
    msg
  }
}