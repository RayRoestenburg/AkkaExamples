package eip.idempotent

import eip.idempotent.IdempotentProtocol.{AddressProtocol, FrameProtocol}

/**
 * The Address of the Envelope.
 */
case class Address(host: String, port: Int, actor: String) {
  def toBytes: Array[Byte] = EnvelopeSerializer.toBytes(this)
  def toProtocol : AddressProtocol= EnvelopeSerializer.toProtocol(this)
}

object Envelope {
  def fromBytes(bytes: Array[Byte]) = EnvelopeSerializer.fromBytes(bytes)
}

object Address{
  def fromBytes(bytes: Array[Byte]) = EnvelopeSerializer.addressFromBytes(bytes)
  def fromProtocol(protocol: AddressProtocol) = EnvelopeSerializer.fromProtocol(protocol)
}

object Frame {
  def fromBytes(bytes: Array[Byte]) = EnvelopeSerializer.frameFromBytes(bytes)
  def fromProtocol(protocol: FrameProtocol) = EnvelopeSerializer.fromProtocol(protocol)
}

/**
 * The Frame containing many envelopes
 */
case class Frame(id: Int, size: Int, returnAddress:Address, address:Address) {
  def toBytes: Array[Byte] = EnvelopeSerializer.toBytes(this)
  def toProtocol : FrameProtocol= EnvelopeSerializer.toProtocol(this)
}

/**
 * The Envelope of the message
 */
case class Envelope(id: Int, frameId: Int) {
  def toBytes: Array[Byte] = EnvelopeSerializer.toBytes(this)
}