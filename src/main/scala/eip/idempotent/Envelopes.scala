package eip.idempotent

import java.lang.String
import collection.mutable.HashMap
import se.scalablesolutions.akka.persistence.redis.RedisStorage
import se.scalablesolutions.akka.stm.local._
import org.jgroups.{Message, ReceiverAdapter, JChannel}

/**
 * Keeps track of Envelopes on the Idempotent Receiver side
 */
trait Envelopes {
  /**
   * isPending returns the envelope for the id passed, that is currently pending (underway and not acknowledged)
   */
  def isPending(id: Long): Option[Envelope]

  /**
   * the envelope is underway, not acknowledged
   */
  def pending(envelope: Envelope): Unit

  /**
   * the envelope has definitely been sent and acknowledged by the Repeater.
   */
  def sent(id: Long): Unit

  def close: Unit = {}
}

/**
 * In Memory Envelopes
 */
class MemEnvelopes extends Envelopes {
  val envelopes = new HashMap[Long, Array[Byte]]

  def isPending(id: Long): Option[Envelope] = {
    envelopes.get(id) match {
      case Some(bytes) => Some(Envelope.fromBytes(bytes))
      case None => None
    }
  }

  def pending(envelope: Envelope): Unit = {
    envelopes += envelope.id -> envelope.toBytes
  }

  def sent(id: Long): Unit = {
    envelopes -= id
  }

}

/**
 * Redis backed Persistent Envelopes
 */
class RedisEnvelopes(storageKey: String) extends Envelopes {
  private lazy val storage = atomic {RedisStorage.getMap(storageKey)}

  def isPending(id: Long): Option[Envelope] = {
    var result: Option[Array[Byte]] = None
    atomic {
      result = storage.get(id.toString.getBytes)
    }
    result match {
      case Some(bytes) => {
        val envelope = Envelope.fromBytes(bytes)
        Some(envelope)
      }
      case None => None
    }
  }

  def pending(envelope: Envelope): Unit = {
    atomic {
      storage.put(envelope.id.toString.getBytes, envelope.toBytes)
      storage.commit
    }
  }

  def sent(id: Long): Unit = {
    atomic {
      storage.remove(id.toString.getBytes)
      storage.commit
    }
  }
}

/**
 * JGroup clustered envelopes.
 */
class JGroupEnvelopes(envelopes: Envelopes, storageKey: String, cluster: String) extends ReceiverAdapter with Envelopes {
  val channel = new JChannel()
  channel.setReceiver(this);
  channel.connect(cluster);

  override def receive(msg: Message): Unit = {
    val bytes = msg.getBuffer
    val syncEnvelope = SyncEnvelope.fromBytes(bytes)
    if(syncEnvelope.sent){
      envelopes.sent(syncEnvelope.envelope.id)
    } else {
      envelopes.pending(syncEnvelope.envelope)
    }
  }
  def isPending(id: Long): Option[Envelope] = {
    envelopes.isPending(id)
  }

  def pending(envelope: Envelope): Unit = {
    envelopes.pending(envelope)
    channel.send(new Message(null, null, new SyncEnvelope(true, envelope).toBytes))
  }

  def sent(id: Long): Unit = {
    envelopes.sent(id)
    //channel.send(new Message(null, null, new SyncEnvelope(false,new Envelope(id, )).toBytes))
  }

  override def close = channel.close
}
