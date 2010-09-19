package eip.idempotent

import java.lang.String
import collection.mutable.HashMap
import se.scalablesolutions.akka.persistence.redis.RedisStorage
import se.scalablesolutions.akka.stm.local._
import org.jgroups.{Message, ReceiverAdapter, JChannel}
import se.scalablesolutions.akka.stm.local._
import collection.JavaConversions.JConcurrentMapWrapper
import java.util.concurrent.ConcurrentHashMap


/**
 * //TODO implement JGroups
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
  def get(frameId:Int, id: Int): Option[Envelope]

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

  def get(frameId:Int, id: Int): Option[Envelope] = {
    envelopes.get((frameId,id))
  }

  def nextFrame(returnAddress: Address, address: Address): Frame = {
    val frame = new Frame(nextFrameId.intValue, frameSize, returnAddress, address)
    frames += frame.id -> frame
    frameCounts
    frame
  }

  def isFrameComplete(frameId: Int): Boolean = {
    frames.get(frameId).exists(frame => frameCounts.getOrElse(frameId,0) >= frame.size)
  }

  def getEnvelopeIds(frameId: Int): Set[Int] = {
    envelopes.filter( pair => pair._2.frameId == frameId).values.map(envelope => envelope.id).toSet
  }

  def getIncompleteFrames(): Set[Frame] = {
    frames.filter(pair=> frameCounts.getOrElse(pair._1,0) < pair._2.size).values.toSet
  }

  def getFrame(frameId: Int): Option[Frame] = {
    frames.get(frameId)
  }


  def put(envelope: Envelope): Boolean = {
    envelopes += (envelope.frameId, envelope.id) -> envelope
    val someCount = frameCounts.get(envelope.frameId)
    var complete = false
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
 * JGroup clustered envelopes.
 */
//class JGroupEnvelopes(envelopes: Envelopes, storageKey: String, cluster: String) extends ReceiverAdapter with Envelopes {
//  val channel = new JChannel()
//  channel.setReceiver(this);
//  channel.connect(cluster);
//
//  override def receive(msg: Message): Unit = {
//    val bytes = msg.getBuffer
//    val syncEnvelope = SyncEnvelope.fromBytes(bytes)
//    if(syncEnvelope.sent){
//      envelopes.remove(syncEnvelope.envelope.id)
//    } else {
//      envelopes.put(syncEnvelope.envelope)
//    }
//  }
//  def get(id: Long): Option[Envelope] = {
//    envelopes.get(id)
//  }
//
//  def put(envelope: Envelope): Unit = {
//    envelopes.put(envelope)
//    channel.send(new Message(null, null, new SyncEnvelope(true, envelope).toBytes))
//  }
//
//  def remove(id: Long): Unit = {
//    envelopes.remove(id)
//    //channel.send(new Message(null, null, new SyncEnvelope(false,new Envelope(id, )).toBytes))
//  }
//
//  override def close = channel.close
//}
