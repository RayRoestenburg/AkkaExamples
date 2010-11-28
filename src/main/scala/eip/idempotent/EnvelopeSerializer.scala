package eip.idempotent

import akka.serialization.Serializer
import com.google.protobuf.{ByteString, Message}
import eip.idempotent.IdempotentProtocol.{FrameProtocol, AddressProtocol, PayloadProtocol, EnvelopeProtocol}

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
    val envelope = new Envelope(envelopeProtocol.getId, envelopeProtocol.getFrameId)
    (envelope, payload)
  }

  /**
   * Adds the message as payload to the envelope and serializes the envelope including the payload
   */
  def serialize(envelope: Envelope, message: Message): EnvelopeProtocol = {
    val payloadBuilder = PayloadProtocol.newBuilder
    payloadBuilder.setMessage(ByteString.copyFrom(message.toByteArray))
    payloadBuilder.setMessageManifest(ByteString.copyFromUtf8(message.getClass.getName))
    val payloadProtocol = payloadBuilder.build
    val envBuilder = EnvelopeProtocol.newBuilder
    envBuilder.setPayload(payloadProtocol)
    envBuilder.setId(envelope.id)
    envBuilder.setFrameId(envelope.frameId)
    envBuilder.build
  }

  /**
   * Creates an Envelope from a byte array (does not include payload, purely the envelope)
   */
  def fromBytes(bytes: Array[Byte]): Envelope = {
    val protocol = EnvelopeProtocol.parseFrom(bytes);
    new Envelope(protocol.getId, protocol.getFrameId)
  }

  /**
   * Creates byte representation of an Envelope only (does not include payload, purely the envelope)
   */
  def toBytes(envelope: Envelope): Array[Byte] = {
    val builder = EnvelopeProtocol.newBuilder
    builder.setId(envelope.id)
    builder.setFrameId(envelope.frameId)
    builder.build.toByteArray
  }

  def toBytes(address:Address) : Array[Byte] = {
    val addressBuilder = AddressProtocol.newBuilder
    addressBuilder.setHost(address.host)
    addressBuilder.setPort(address.port)
    addressBuilder.setActor(address.actor)
    addressBuilder.build.toByteArray
  }
  def addressFromBytes(bytes: Array[Byte]): Address = {
    val protocol = AddressProtocol.parseFrom(bytes);
    new Address(protocol.getHost, protocol.getPort, protocol.getActor)
  }
  def frameFromBytes(bytes: Array[Byte]): Frame = {
    val protocol = FrameProtocol.parseFrom(bytes);
    fromProtocol(protocol)
  }
  def fromProtocol(protocol:FrameProtocol): Frame = {
    new Frame(protocol.getId, protocol.getSize, fromProtocol(protocol.getReturnAddress), fromProtocol(protocol.getAddress))
  }

  def toBytes(frame:Frame) : Array[Byte] = {
    toProtocol(frame).toByteArray
  }
  def toProtocol(address:Address) :AddressProtocol= {
    val addressBuilder = AddressProtocol.newBuilder
    addressBuilder.setActor(address.actor)
    addressBuilder.setHost(address.host)
    addressBuilder.setPort(address.port)
    addressBuilder.build
  }

  def fromProtocol(protocol: AddressProtocol) : Address = {
    new Address(protocol.getHost, protocol.getPort, protocol.getActor)
  }
  
  def toProtocol(frame:Frame) : FrameProtocol = {
    val frameBuilder = FrameProtocol.newBuilder
    frameBuilder.setId(frame.id)
    frameBuilder.setSize(frame.size)
    frameBuilder.setReturnAddress(toProtocol(frame.returnAddress))
    frameBuilder.setAddress(toProtocol(frame.address))
    frameBuilder.build
  }

  private def loadManifest(classLoader: Option[ClassLoader], payloadProtocol: PayloadProtocol): Class[_] = {
    val manifest = payloadProtocol.getMessageManifest.toStringUtf8
    if (classLoader.isDefined) classLoader.get.loadClass(manifest)
    else Class.forName(manifest)
  }
}