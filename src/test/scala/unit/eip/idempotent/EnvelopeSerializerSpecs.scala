package unit.eip.idempotent

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.Spec
import unit.test.proto.Commands
import eip.idempotent.{Address, Envelope, EnvelopeSerializer}
import eip.idempotent.IdempotentProtocol.EnvelopeProtocol
import com.google.protobuf.Message
import unit.test.proto.Commands.WorkerCommand

/**
 * Test for the envelope serializer
 */
class EnvelopeSerializerSpecs extends Spec with ShouldMatchers {
  describe("The EnvelopeSerializer") {

    describe("(when it serializes an Envelope)") {
      it("should serialize the payload and envelope and deserialize correctly") {
        val message = Commands.WorkerCommand.newBuilder.setId(1).setName("command-1").setData("data-1").build
        val envelope = new Envelope(1, 3)
        val protocol = EnvelopeSerializer.serialize(envelope, message)
        val bytes = protocol.toByteArray
        val result = EnvelopeProtocol.parseFrom(bytes)
        val res :(Envelope, Any) = EnvelopeSerializer.deserialize(result)
        val resultEnvelope = res._1
        val resultMessage  = res._2.asInstanceOf[WorkerCommand]
        resultMessage.getId should equal(message.getId)
        resultMessage.getName should equal(message.getName)
        resultMessage.getData should equal(message.getData)
        resultEnvelope.id should equal(envelope.id)
        resultEnvelope.frameId should equal(envelope.frameId)
      }
    }
  }
}