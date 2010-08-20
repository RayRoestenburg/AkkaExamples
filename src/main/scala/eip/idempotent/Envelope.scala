package eip.idempotent

import java.lang.String
import sbinary.{Output, Input, Format}
import se.scalablesolutions.akka.serialization.{Serializable}
//TODO use the EnvelopeSerializer instead
//TODO implement JGroupEnvelopes and test
//TODO improve Repeater, IdempotentReceiver and test
//TODO add all other messages to protobuf protocol
object Envelope {
  import sbinary.DefaultProtocol._
  import sbinary.Operations._
  import Sender.SenderFormat.{reads => readSender}
  import Sender.SenderFormat.{writes => writeSender}

  implicit object EnvelopeFormat extends Format[Envelope] {
    def reads(in: Input) = Envelope(
      read[Long](in),
      read[Long](in), readSender(in))

    def writes(out: Output, value: Envelope) = {
      write[Long](out, value.id)
      write[Long](out, value.timestamp)
      writeSender(out, value.sender)
    }
  }
  def fromBytes(bytes: Array[Byte]) = fromByteArray[Envelope](bytes)
}
object SyncEnvelope {
  import sbinary.DefaultProtocol._
  import sbinary.Operations._
  import Envelope.EnvelopeFormat.{reads => readEnvelope}
  import Envelope.EnvelopeFormat.{writes => writeEnvelope}

  implicit object SyncEnvelopeFormat extends Format[SyncEnvelope] {
    def reads(in: Input) = SyncEnvelope(
      read[Boolean](in),
      readEnvelope(in))

    def writes(out: Output, value: SyncEnvelope) = {
      write[Boolean](out, value.sent)
      writeEnvelope(out, value.envelope)
    }
  }
  def fromBytes(bytes: Array[Byte]) = fromByteArray[SyncEnvelope](bytes)
}

object Sender {
  import sbinary.DefaultProtocol._
  import sbinary.Operations._
  implicit object SenderFormat extends Format[Sender] {
    def reads(in: Input) = Sender(
      read[String](in),
      read[Int](in),
      read[String](in))

    def writes(out: Output, value: Sender) = {
      write[String](out, value.host)
      write[Int](out, value.port)
      write[String](out, value.actor)
    }
  }
  def fromBytes(bytes: Array[Byte]) = fromByteArray[Sender](bytes)
}

/**
 * The Sender of the Envelope, so that the IdempotentReceiver can send Acknowledgements back to it.
 */
case class Sender(host: String, port: Int, actor: String) extends Serializable.SBinary[Sender] {
  import sbinary.DefaultProtocol._
  import sbinary.Operations._
  def this() = this (null, 0, null)

  def fromBytes(bytes: Array[Byte]) = fromByteArray[Sender](bytes)

  def toBytes: Array[Byte] = toByteArray(this)
}

/**
 * The Envelope of the message
 */
case class Envelope(id: Long, timestamp: Long, sender: Sender) extends Serializable.SBinary[Envelope] {
  import sbinary.DefaultProtocol._
  import sbinary.Operations._
  def this() = this (0, 0, new Sender())

  def fromBytes(bytes: Array[Byte]) = fromByteArray[Envelope](bytes)

  def toBytes: Array[Byte] = toByteArray(this)
}

/**
 * SyncEnvelope message between clustered Idempotent Receivers
 */
case class SyncEnvelope(sent: Boolean, envelope:Envelope) extends Serializable.SBinary[SyncEnvelope] {
  import sbinary.DefaultProtocol._
  import sbinary.Operations._
  def this() = this (false, new Envelope())

  def fromBytes(bytes: Array[Byte]) = fromByteArray[SyncEnvelope](bytes)

  def toBytes: Array[Byte] = toByteArray(this)
}
case class TrackedMessage(envelope:Envelope, message:Any)