package unit.eip.idempotent

import se.scalablesolutions.akka.util.Logging
import org.scalatest.{Spec, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import eip.idempotent._

class EnvelopesSpecs extends Spec with ShouldMatchers with BeforeAndAfterAll with Logging {
  def createEnvelopes: Envelopes = {
    new MemEnvelopes(1,1000, 10)
  }
  describe("when all envelopes are added for the frame") {
    it("should be complete and possible to remove the frame") {
      val envelopes = createEnvelopes
      envelopes.clear
      for (i <- 0 until 9) {
        envelopes.put(new Envelope(i, 1)) should equal (false)
      }
      envelopes.put(new Envelope(9, 1)) should equal (true)
      envelopes.removeFrame(1) should equal(true)
    }
  }
  describe("when an envelope is put") {
    it("should be present") {
      val envelopes = createEnvelopes
      envelopes.clear
      val env = new Envelope(13, 1)
      envelopes.put(env)
      val someEnvelope = envelopes.get(env.frameId, env.id,null)
      someEnvelope match {
        case Some(envelope) => {
          envelope.id should equal(env.id)
          envelope.frameId should equal(env.frameId)
        }
        case None => fail("envelope not found")
      }
    }
  }
  describe("when an envelope is cleared") {
    it("should be clear") {
      val envelopes = createEnvelopes
      envelopes.clear
      val env = new Envelope(13, 1)
      envelopes.put(env)
      envelopes.size should equal(1)
      envelopes.clear
      envelopes.removeFrame(1) should equal(false)
      envelopes.get(env.frameId, env.id,null) should equal(None)
      envelopes.size should equal(0)
    }
  }
  describe("when a new frame is requested") {
    it("should increment with every request") {
      val envelopes = createEnvelopes
      envelopes.clear
      val returnAddress = new Address("localhost", 1001, "actor")
      val address = new Address("remotehost", 1001, "actor")
      var frame = envelopes.nextFrame(returnAddress, address)
      val id = frame.id
      frame.size should equal (10)
      frame = envelopes.nextFrame(returnAddress, address)
      frame.id should equal (id+1)
      frame.size should equal (10)
    }
  }
}