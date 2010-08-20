package eip.idempotent

import se.scalablesolutions.akka.actor.Actor._
import se.scalablesolutions.akka.remote._
import se.scalablesolutions.akka.actor.{ActorRef, Actor}
import java.lang.String
import eip.idempotent.IdempotentProtocol.EnvelopeProtocol

object IdempotentReceiver {
  val server = new RemoteServer
  var host: String = "localhost"
  var port: Int = 18095

  def idempotent(remoteActorName: String, actorRef: ActorRef, envelopes: Envelopes): ActorRef = {
    if (!server.isRunning) {
      server.start(host, port)
      Thread.sleep(1000)
    }
    if (!actorRef.isRunning) {
      actorRef.start
    }
    val idempotentActorRef = actorOf(new IdempotentReceiver(Set(actorRef), envelopes))
    server.register(remoteActorName, idempotentActorRef)
    idempotentActorRef
  }
}

class IdempotentReceiver(actors: Set[ActorRef], envelopes: Envelopes) extends Actor {
  def receive = {
    case msg: EnvelopeProtocol => {
      val protomsg: (Envelope, Any) = EnvelopeSerializer.deserialize(msg)
      val envelope = protomsg._1
      val payload = protomsg._2

      val someValue = envelopes.isPending(envelope.id)
      someValue match {
        case Some(envelope: Envelope) => {
          log.info("LOOP: Received already known envelope:  " + envelope.id)
          // check repeat flag, if repeat flag, broadcast sync
          self.reply_?(new IgnoredEnvelope(envelope.id))
        }
        case None => {
          envelopes.pending(envelope)
          for (actor <- actors) {
            log.info("LOOP: passing message to actor:  " + envelope.id)
            actor ! payload
          }
          val repeater = RemoteClient.actorFor(envelope.sender.actor, envelope.sender.host, envelope.sender.port)
          val future = repeater !!! new Acknowledgements(Set(envelope.id))
          spawn {
            future.await
            val result :Option[AcknowledgementsProcessed]= future.result

            result match {
            //envelopes.sent should be relayed
              case Some(msg: AcknowledgementsProcessed) => { msg.ids}
              case None => {}
            }
          }
          log.info("LOOP: sent ack to repeater for id  " + envelope.id)
        }
      }
    }
  }
}

case class IgnoredEnvelope(id: Long)
case class Acknowledgements(ids: Set[Long])
case class AcknowledgementsProcessed(ids: Set[Long])
case class BufferSizeResponse(size: Int)
class BufferSizeRequest