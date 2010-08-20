package eip.idempotent

import se.scalablesolutions.akka.serialization.Serializer
import com.google.protobuf.{ByteString, Message}
import eip.idempotent.IdempotentProtocol.{SenderProtocol, PayloadProtocol, EnvelopeProtocol}

/**
 * Serializer to serialize and deserialize the Envelope and any protobuf message that it contains,
 * works similar to MessageSerializer in akka.  Right now only supports protobuf serialization for simplicity.
 */
object EnvelopeSerializer {
  private var SERIALIZER_PROTOBUF: Serializer.Protobuf = Serializer.Protobuf

  /**
   * Deserializes the envelope and returns the envelope and the enclosed payload
   */
  def deserialize(envelopeProtocol: EnvelopeProtocol): (Envelope, Any) = {

    val clazz = loadManifest(SERIALIZER_PROTOBUF.classLoader, envelopeProtocol.getPayload)
    val payload = SERIALIZER_PROTOBUF.fromBinary(envelopeProtocol.getPayload.getMessage.toByteArray, Some(clazz))
    val sender = new Sender(envelopeProtocol.getSender.getHost, envelopeProtocol.getSender.getPort, envelopeProtocol.getSender.getActor)
    val envelope = new Envelope(envelopeProtocol.getId, envelopeProtocol.getTimestamp, sender)
    (envelope, payload)
  }

  /**
   * Adds the message as payload to the envelope and serializes the envelope including the payload
   */
  def serialize(envelope: Envelope, message: Message): EnvelopeProtocol = {
    val payloadBuilder = PayloadProtocol.newBuilder
    payloadBuilder.setMessage(ByteString.copyFrom(message.toByteArray))
    payloadBuilder.setMessageManifest(ByteString.copyFromUtf8(message.getClass.getName))
    val senderBuilder = SenderProtocol.newBuilder
    senderBuilder.setHost(envelope.sender.host)
    senderBuilder.setActor(envelope.sender.actor)
    senderBuilder.setPort(envelope.sender.port)
    val senderProtocol = senderBuilder.build
    val payloadProtocol = payloadBuilder.build
    val envBuilder = EnvelopeProtocol.newBuilder
    envBuilder.setPayload(payloadProtocol)
    envBuilder.setId(envelope.id)
    envBuilder.setTimestamp(envelope.timestamp)
    envBuilder.setSender(senderProtocol)
    envBuilder.build
  }

  /**
   * Creates an Envelope from a byte array (does not include payload, purely the envelope)
   */
  def fromBytes(bytes: Array[Byte]): Envelope = {
    val protocol = EnvelopeProtocol.parseFrom(bytes);
    new Envelope(protocol.getId, protocol.getTimestamp, new Sender(protocol.getSender.getHost, protocol.getSender.getPort, protocol.getSender.getActor))
  }

  /**
   * Creates byte representation of an Envelope only (does not include payload, purely the envelope)
   */
  def toBytes(envelope: Envelope): Array[Byte] = {
    val builder = EnvelopeProtocol.newBuilder
    builder.setId(envelope.id)
    builder.setTimestamp(envelope.timestamp)
    val senderBuilder = SenderProtocol.newBuilder
    senderBuilder.setHost(envelope.sender.host)
    senderBuilder.setPort(envelope.sender.port)
    senderBuilder.setActor(envelope.sender.actor)
    val sender = senderBuilder.build
    builder.setSender(sender)
    builder.build.toByteArray
  }

  private def loadManifest(classLoader: Option[ClassLoader], payloadProtocol: PayloadProtocol): Class[_] = {
    val manifest = payloadProtocol.getMessageManifest.toStringUtf8
    if (classLoader.isDefined) classLoader.get.loadClass(manifest)
    else Class.forName(manifest)
  }
}