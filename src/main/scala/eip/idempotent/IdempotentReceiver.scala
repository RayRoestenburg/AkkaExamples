package eip.idempotent

import se.scalablesolutions.akka.actor.Actor._
import se.scalablesolutions.akka.remote._
import se.scalablesolutions.akka.actor.{ActorRef, Actor}
import java.lang.String
import eip.idempotent.IdempotentProtocol._
import collection.mutable.HashMap
import java.net.InetAddress
import se.scalablesolutions.akka.util.Logging
import collection.JavaConversions.JConcurrentMapWrapper
import java.util.concurrent.ConcurrentHashMap
import com.eaio.uuid.UUID


class IdempotentServer(envelopes: Envelopes, timeout: Int) {
  private val idempotentActors = new JConcurrentMapWrapper(new ConcurrentHashMap[String, ActorRef]())
  private var _host = "localhost"
  private var _port = 0
  private val server = new RemoteServer
  private val repeatFrameRequester = actorOf(new RepeatFrameRequester(envelopes, timeout))
  repeatFrameRequester.start
  private val listener = actorOf(new ReceiverConnectionListener(repeatFrameRequester, envelopes, timeout))
  listener.start
  server.addListener(listener)
  envelopes.setServer(this)
  
  def start(host: String, port: Int) = {
    server.start(host, port)
    _host = host
    _port = port
  }

  def idempotentActorRef(remoteActorName: String):Option[ActorRef] = {
    idempotentActors.get(remoteActorName)
  }

  def register(remoteActorName: String, actorRef: ActorRef): ActorRef = {
    if (!idempotentActors.contains(remoteActorName)) {
      if (!actorRef.isRunning) {
        actorRef.start
      }
      val idempotentActorRef = actorOf(new IdempotentReceiver(Set(actorRef), envelopes, _host, _port))
      idempotentActorRef.start
      server.register(remoteActorName, idempotentActorRef)
      idempotentActors+= remoteActorName ->idempotentActorRef
      idempotentActorRef
    } else {
      throw new Exception("Remote Actor "+remoteActorName+" is already active as Idempotent Receiver")
    }
  }

  def unregister(remoteActorName: String) = {
    server.unregister(remoteActorName);
    idempotentActors.remove(remoteActorName)
  }

  def shutdown = {
    try {
      envelopes.close
      server.removeListener(listener)
    } finally {
      server.shutdown
    }
  }
}

class ReceiverConnectionListener(repeatFrameRequester: ActorRef, envelopes: Envelopes, timeout: Int) extends Actor {
  private var serverError = false
  private var serverDisconnected = false
  private var clientDisconnected = false
  private var clientError = false

  def receive = {
    case RemoteClientError(cause, client: RemoteClient) => {
      clientError = true;
    }
    case RemoteClientDisconnected(client: RemoteClient) => {
      log.debug("Remote client %s:%d disconnected from receiver", client.hostname, client.port)
      clientDisconnected = true
    }
    case RemoteClientConnected(client: RemoteClient) => {
      if (clientDisconnected || clientError) {
        log.debug("Remote client %s:%d reconnected to receiver", client.hostname, client.port)
        // connected after disconnect or error
        repeatFrameRequester ! ReconnectClient(self, client)
        clientError = false
        clientDisconnected = false
      }
    }
    case RemoteClientShutdown(client) => {
    }

    case RemoteServerError(cause, server: RemoteServer) => {
      log.debug("Remote Server error on receiver %s", server.name)
      serverError = true
    }
    case RemoteServerShutdown(server) => {
    }
    case RemoteServerStarted(server) => {
      log.debug("Remote Server started %s", server.name)
      // start handling incomplete requests at startup (if envelopes are persistent)
      repeatFrameRequester ! ReconnectServer(self, server)
    }
    case RemoteServerClientConnected(server) => {
      log.debug("Remote Server Client connected to server %s", server.name)
      if (serverDisconnected || serverError) {
        // connected after disconnected
        repeatFrameRequester ! ReconnectServer(self, server)
        serverDisconnected = false
        serverError = false
      }
    }
    case RemoteServerClientDisconnected(server: RemoteServer) => {
      log.debug("Remote Server Client disconnected to server %s", server.name)
      // set flag for disconnect of clients
      serverDisconnected = true
    }
  }
}
case class ReconnectServer(listener: ActorRef, server: RemoteServer)
case class ReconnectClient(listener: ActorRef, client: RemoteClient)


class RepeatFrameRequester(envelopes: Envelopes, timeout: Int) extends Actor with Logging {
  private var clients = Set[Address]()

  def receive = {
    case msg: ReconnectServer => {
      reconnect(msg.listener)
    }
    case msg: ReconnectClient => {
      reconnect(msg.listener)
    }
  }

  def reconnect(listener: ActorRef) = {
    val frames = envelopes.getIncompleteFrames
    for (frame <- frames) {
      log.info("reconnecting for frame %d, to client host: %s, port %d, %s", frame.id, frame.returnAddress.host, frame.returnAddress.port, frame.returnAddress.actor)
      val envelopeIds = envelopes.getEnvelopeIds(frame.id)
      val frameProtocol = EnvelopeSerializer.toProtocol(frame)
      //sender in frame should always be the same one
      val builder = RepeatFrameRequestProtocol.newBuilder
      for (envelopeId <- envelopeIds) {
        builder.addEnvelope(envelopeId)
      }
      val repeatFrame = builder.setFrame(frameProtocol).build

      val client = RemoteClient.clientFor(frame.returnAddress.host, frame.returnAddress.port)

      if (!clients.contains(frame.returnAddress)) {
        clients = clients + frame.returnAddress
        client.addListener(listener)
      }

      var actorRef = RemoteClient.actorFor(frame.returnAddress.actor, frame.returnAddress.host, frame.returnAddress.port)
      var success = false
      while (!success) {
        if(actorRef.isShutdown){
	  actorRef = RemoteClient.actorFor(frame.returnAddress.actor, frame.returnAddress.host, frame.returnAddress.port) 
        }
        val reply = actorRef !! (repeatFrame, timeout)
        reply match {
          case Some(response: RepeatFrameResponseProtocol) => {
            success = true
            // keep track of frame amount handled, and remove envelopeIds from buffer
          }
          case None => {
            log.info("Timeout on RepeatFrameRequest for frame %s", frame.id)
          }
        }
      }
    }
  }
}

class IdempotentReceiver(actors: Set[ActorRef], envelopes: Envelopes, host: String, port: Int) extends Actor {
  def completeFrame(envelope: Envelope): Unit = {
    val someFrame = envelopes.getFrame(envelope.frameId)
    someFrame match {
      case Some(frame) => {
        spawn {
          log.debug("Completing frame %d to %s,%s,%d", frame.id, frame.returnAddress.actor, frame.returnAddress.host, frame.returnAddress.port)
          var repeater = RemoteClient.actorFor(frame.returnAddress.actor, frame.returnAddress.host, frame.returnAddress.port)

          val completeFrameMsg = CompleteFrameRequestProtocol.newBuilder.setFrame(frame.toProtocol).build
          var success = false

          while (!success) {
            if(repeater.isShutdown) {
              repeater = RemoteClient.actorFor(frame.returnAddress.actor, frame.returnAddress.host, frame.returnAddress.port)
	    }
            val reply = repeater !! completeFrameMsg
            reply match {
              case Some(response: CompleteFrameResponseProtocol) => {
                success = true
                // keep track of frame amount handled, and remove envelopeIds from buffer
                envelopes.removeFrame(envelope.frameId)
              }
              case None => {
                log.info("Timeout on CompleteFrameRequest for frame %s", frame.id)
              }
            }
          }
        }
      }
      case None => log.error("Frame %d for envelope %d not known when trying to complete frame", envelope.frameId, envelope.id)
    }
  }

  def receive = {
    case msg: EnvelopeProtocol => {
      // TODO check if sender sent this with !, !!, or !!!
      val protomsg: (Envelope, Any) = EnvelopeSerializer.deserialize(msg)
      val envelope = protomsg._1
      val payload = protomsg._2

      val someValue = envelopes.get(envelope.frameId, envelope.id, payload)
      someValue match {
        case Some(envelope: Envelope) => {
          log.debug("Ignored envelope %d, frame %d, on %s:%d", envelope.id, envelope.frameId, host, port)
          // repeat the previous response, if it is a !! or !!!
        }
        case None => {
          log.debug("Received new envelope %d, frame %d, on %s:%d", envelope.id, envelope.frameId, host, port)
          val complete = envelopes.put(envelope)
          for (actor <- actors) {
            actor ! payload
          }
          if (complete) {
            completeFrame(envelope)
          }
        }
      }
    }
    case msg: FrameRequestProtocol => {
      log.debug("Received FrameRequest from %s@%s:%d.", msg.getReturnAddress.getActor, msg.getReturnAddress.getHost, msg.getReturnAddress.getPort)
      val returnAddress = EnvelopeSerializer.fromProtocol(msg.getReturnAddress)
      val address = EnvelopeSerializer.fromProtocol(msg.getAddress)
      val frame = envelopes.nextFrame(returnAddress, address)
      log.debug("Created new Frame for %s@%s:%d to %s@%s:%d, id %d, size %d.", frame.returnAddress.actor, frame.returnAddress.host,
        frame.returnAddress.port, frame.address.actor, frame.address.host, frame.address.port, frame.id, frame.size)
      self.reply(FrameResponseProtocol.newBuilder.setFrame(frame.toProtocol).build)
    }
  }
}
