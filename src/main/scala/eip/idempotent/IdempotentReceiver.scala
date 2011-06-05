package eip.idempotent

import akka.actor.Actor._
import akka.remoteinterface._
import akka.actor.{ActorRef, Actor}
import java.lang.String
import eip.idempotent.IdempotentProtocol._
import collection.JavaConversions.JConcurrentMapWrapper
import java.util.concurrent.ConcurrentHashMap
import java.net.{InetSocketAddress}
import akka.event.slf4j.Logging

/**
 * Idempotent server. wraps Actors into IdempotentReceiver Actors so that they can be accessed with the RepeaterClient.
 */
class IdempotentServer(envelopes: Envelopes, timeout: Int) {
  private val idempotentActors = new JConcurrentMapWrapper(new ConcurrentHashMap[String, ActorRef]())
  private var _host = "localhost"
  private var _port = 0
  var remoteModule = remote
  private val repeatFrameRequester = actorOf(new RepeatFrameRequester(envelopes, timeout))
  repeatFrameRequester.start
  private val listener = actorOf(new ReceiverConnectionListener(repeatFrameRequester, envelopes, timeout))
  listener.start
  envelopes.setServer(this)

  def start(host: String, port: Int) = {
    if (!remoteModule.isRunning) {
      remoteModule.addListener(listener)
      remoteModule.start(host, port)
      _host = host
      _port = port
    }
  }

  def start(host: String, port: Int, loader: ClassLoader) = {
    if (!remoteModule.isRunning) {
      remoteModule.addListener(listener)
      remoteModule.start(host, port, loader)
      _host = host
      _port = port
    }
  }

  def idempotentActorRef(remoteActorName: String): Option[ActorRef] = {
    idempotentActors.get(remoteActorName)
  }

  def register(remoteActorName: String, actorRef: ActorRef): ActorRef = {
    if (!idempotentActors.contains(remoteActorName)) {
      if (!actorRef.isRunning) {
        actorRef.start
      }
      val idempotentActorRef = actorOf(new IdempotentReceiver(Set(actorRef), envelopes, _host, _port))
      idempotentActorRef.start
      remote.register(remoteActorName, idempotentActorRef)
      idempotentActors += remoteActorName -> idempotentActorRef
      idempotentActorRef
    } else {
      throw new Exception("Remote Actor " + remoteActorName + " is already active as Idempotent Receiver")
    }
  }

  def unregister(remoteActorName: String) = {
    remoteModule.unregister(remoteActorName);
    idempotentActors.remove(remoteActorName)
  }

  def shutdown = {
    try {
      envelopes.close
      remoteModule.removeListener(listener)
    } finally {
      remoteModule.shutdown
    }
  }
}

/**
 * Connection Listener that triggers requests to the repeater, to repeat frame because an error has occurred.
 */
class ReceiverConnectionListener(repeatFrameRequester: ActorRef, envelopes: Envelopes, timeout: Int) extends Actor with Logging {
  private var serverError = false
  private var serverDisconnected = false
  private var clientDisconnected = false
  private var clientError = false

  def receive = {
    case RemoteClientError(cause, client: RemoteClientModule, remoteAddress) => {
      clientError = true;
    }
    case RemoteClientDisconnected(client: RemoteClientModule, remoteAddress: InetSocketAddress) => {
      log.debug("Remote client %s:%d disconnected from receiver", remoteAddress.getHostName, remoteAddress.getPort)
      clientDisconnected = true
    }
    case RemoteClientConnected(client: RemoteClientModule, remoteAddress: InetSocketAddress) => {
      if (clientDisconnected || clientError) {
        log.debug("Remote client %s:%d reconnected to receiver", remoteAddress.getHostName, remoteAddress.getPort)
        // connected after disconnect or error
        repeatFrameRequester ! ReconnectClient(self, client)
        clientError = false
        clientDisconnected = false
      }
    }
    case RemoteClientShutdown(client, remoteAddress) => {
    }
    case RemoteClientWriteFailed(request, error: Throwable, client: RemoteClientModule, address: InetSocketAddress) => {
      clientError = true
    }
    case RemoteServerError(cause, server: RemoteServerModule) => {
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
    case RemoteServerClientConnected(server, address: Option[InetSocketAddress]) => {
      log.debug("Remote Server Client connected to server %s", server.name)
      if (serverDisconnected || serverError) {
        // connected after disconnected
        repeatFrameRequester ! ReconnectServer(self, server)
        serverDisconnected = false
        serverError = false
      }
    }
    case RemoteServerClientClosed(server: RemoteServerModule, clientAddress: Option[InetSocketAddress]) => {
    }
    case RemoteServerClientDisconnected(server: RemoteServerModule, clientAddress: Option[InetSocketAddress]) => {
      log.debug("Remote Server Client disconnected to server %s", server.name)
      // set flag for disconnect of clients
      serverDisconnected = true
    }
  }
}

case class ReconnectServer(listener: ActorRef, server: RemoteServerModule)

case class ReconnectClient(listener: ActorRef, client: RemoteClientModule)

/**
 * Requests the repeater to repeat frames when there is a reconnect
 */
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
      log.info("reconnecting for frame %d, to client host: %s, port %d, %s".format(frame.id, frame.returnAddress.host, frame.returnAddress.port, frame.returnAddress.actor))
      val envelopeIds = envelopes.getEnvelopeIds(frame.id)
      val frameProtocol = EnvelopeSerializer.toProtocol(frame)
      //sender in frame should always be the same one
      val builder = RepeatFrameRequestProtocol.newBuilder
      for (envelopeId <- envelopeIds) {
        builder.addEnvelope(envelopeId)
      }
      val repeatFrame = builder.setFrame(frameProtocol).build

      if (!clients.contains(frame.returnAddress)) {
        clients = clients + frame.returnAddress
        remote.addListener(listener)
      }

      var actorRef = remote.actorFor(frame.returnAddress.actor, frame.returnAddress.host, frame.returnAddress.port)
      var success = false
      while (!success) {
        if (actorRef.isShutdown) {
          actorRef = remote.actorFor(frame.returnAddress.actor, frame.returnAddress.host, frame.returnAddress.port)
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

/**
 * Idempotent Receiver. ignores duplicate messages, handles frame requests and communicates with
 * the repeater when frames are completed. Only works for oneway messages at the moment.
 */
class IdempotentReceiver(actors: Set[ActorRef], envelopes: Envelopes, host: String, port: Int) extends Actor with Logging {
  def completeFrame(envelope: Envelope): Unit = {
    val someFrame = envelopes.getFrame(envelope.frameId)
    someFrame match {
      case Some(frame) => {
        spawn {
          log.debug("Completing frame %d to %s,%s,%d".format(frame.id, frame.returnAddress.actor, frame.returnAddress.host, frame.returnAddress.port))
          var repeater = remote.actorFor(frame.returnAddress.actor, frame.returnAddress.host, frame.returnAddress.port)

          val completeFrameMsg = CompleteFrameRequestProtocol.newBuilder.setFrame(frame.toProtocol).build
          var success = false

          while (!success) {
            if (repeater.isShutdown) {
              repeater = remote.actorFor(frame.returnAddress.actor, frame.returnAddress.host, frame.returnAddress.port)
            }
            val reply = repeater !! completeFrameMsg
            reply match {
              case Some(response: CompleteFrameResponseProtocol) => {
                success = true
                // keep track of frame amount handled, and remove envelopeIds from buffer
                envelopes.removeFrame(envelope.frameId)
              }
              case None => {
                log.info("Timeout on CompleteFrameRequest for frame %s".format(frame.id))
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
          log.debug("Ignored envelope %d, frame %d, on %s:%d".format(envelope.id, envelope.frameId, host, port))
          // repeat the previous response, if it is a !! or !!!
        }
        case None => {
          log.debug("Received new envelope %d, frame %d, on %s:%d".format(envelope.id, envelope.frameId, host, port))
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
      log.debug("Received FrameRequest from %s@%s:%d.".format(msg.getReturnAddress.getActor, msg.getReturnAddress.getHost, msg.getReturnAddress.getPort))
      val returnAddress = EnvelopeSerializer.fromProtocol(msg.getReturnAddress)
      val address = EnvelopeSerializer.fromProtocol(msg.getAddress)
      val frame = envelopes.nextFrame(returnAddress, address)
      log.debug("Created new Frame for %s@%s:%d to %s@%s:%d, id %d, size %d.".format(frame.returnAddress.actor, frame.returnAddress.host,
        frame.returnAddress.port, frame.address.actor, frame.address.host, frame.address.port, frame.id, frame.size))
      self.reply(FrameResponseProtocol.newBuilder.setFrame(frame.toProtocol).build)
    }
  }
}
