package tools

import java.util.concurrent.CyclicBarrier
import akka.util.Logging
import java.io._
import akka.actor.Actor._
import java.net.{SocketException, ServerSocket, Socket}

/**
 * A very simple (Blocking I/O) Network Proxy that will be used to simulate network errors.
 * The proxy sits between the remote actors (between "client" and "server"). The proxy can be stopped and started
 * (blocking methods on a barrier for ease of use in testing, so that you are sure its started or stopped),
 * to simulate network disconnects.
 */

class NetworkProxy extends Logging {
  private var localPort: Int = 18001
  private var proxyRunnable: ProxyRunnable = null
  private var host: String = "localhost"
  private var remotePort = 8001
  private var started = false

  def this(host: String, localPort: Int, remotePort: Int) {
    this ()
    this.host = host
    this.localPort = localPort
    this.remotePort = remotePort
    proxyRunnable = new ProxyRunnable(host, localPort, remotePort)
  }

  def stop: Unit = {
    if (started) {
      log.info("stopping network proxy")
      proxyRunnable.stop
      log.info("stopped network proxy")
      started = false
    }
  }

  def start: Unit = {
    if (!started) {
      log.info("starting network proxy %s local port: %s remote port: %s", host, localPort, remotePort)
      var t: Thread = new Thread(proxyRunnable)
      t.start
      proxyRunnable.waitUntilStarted
      log.info("started network proxy")
      started = true
    }
  }
  def injectClientFunction(func: (Socket, Socket, InputStream, OutputStream) => Unit ) : Unit = {
     proxyRunnable.injectClientFunction(func)
  }
  def injectServerFunction(func: (Socket, Socket, InputStream, OutputStream) => Unit) : Unit = {
    proxyRunnable.injectServerFunction(func)
  }
  def clearInjectedFunctions : Unit = {
    proxyRunnable.clearInjectedFunctions
  }
}


/**
 * ProxyRunnable that runs the proxy.
 */
class ProxyRunnable extends Runnable with Logging {
  private var host = "localhost"
  private var localPort = 18001
  private var serverSocket: ServerSocket = null
  private var disconnected = false
  private var remotePort = 8001
  private val stopBarrier = new CyclicBarrier(2)
  private val startBarrier = new CyclicBarrier(2)
  private val emptyFunction: (Socket, Socket, InputStream, OutputStream) => Unit  =  (_,_,_,_) => ();
  private var clientFunction: (Socket, Socket, InputStream, OutputStream) => Unit = emptyFunction
  private var serverFunction: (Socket, Socket, InputStream, OutputStream) => Unit = emptyFunction
  private[tools] def this(host: String, localPort: Int, remotePort: Int) {
    this ()
    this.host = host
    this.localPort = localPort
    this.remotePort = remotePort
  }

  implicit def socket2Closable(socket: Socket): Closeable = new Closeable() {
    def close() = {
      socket.close
    }
  }

  implicit def serverSocket2Closable(serverSocket: ServerSocket): Closeable = new Closeable() {
    def close() = {
      serverSocket.close
    }
  }

  def close(closeables: Closeable*): Unit = {using(closeables: _*) {}}

  def using(closeables: Closeable*)(body: (Unit)) = {
    try {
      body
    }
    finally {
      for (closeable <- closeables) {
        try {
          if (closeable != null) closeable.close
        } catch {
          case e: IOException => {
            log.error(e, "IOException in closing resource.")
          }
        }
      }
    }
  }
  def injectServerFunction(errorThrowingFunction: (Socket, Socket, InputStream, OutputStream) => Unit) :Unit = {
    serverFunction = errorThrowingFunction
  }
  def injectClientFunction(errorThrowingFunction: (Socket, Socket, InputStream, OutputStream) => Unit) :Unit = {
    clientFunction = errorThrowingFunction
  }

  def clearInjectedFunctions : Unit = {
    clientFunction = emptyFunction
    serverFunction = emptyFunction
  }
  def connectClientToRemoteServer(client: Socket): (Socket, InputStream, OutputStream, InputStream, OutputStream) = {
    try {
      val streamFromClient: InputStream = client.getInputStream
      val streamToClient: OutputStream = client.getOutputStream
      val server = new Socket(host, remotePort)
      val streamFromServer: InputStream = server.getInputStream
      val streamToServer: OutputStream = server.getOutputStream
      return (server, streamFromClient, streamToClient, streamFromServer, streamToServer)
    }
    catch {
      case e: IOException => {
        log.error(e, "IOException in network proxy, connecting to remote %s:%s.", host, remotePort)
        val p: PrintWriter = new PrintWriter(client.getOutputStream)
        using(p, client) {
          p.print("Proxy server cannot connect to " + host + ":" + remotePort + ":\n" + e + "\n")
          p.flush
        }
        throw e
      }
    }
  }

  def streamClientRequestToServer(client: Socket, server: Socket, streamFromClient: InputStream, streamToServer: OutputStream): Unit = {
    val request: Array[Byte] = new Array[Byte](1024)
    var bytesRead: Int = 0
    var first = false;
    try {
      using(streamFromClient, streamToServer) {
        while (!disconnected && (({
          bytesRead = streamFromClient.read(request);
          bytesRead
        })) != -1) {
          clientFunction(client, server, streamFromClient, streamToServer)
          if (!first) {
            if (new String(request).startsWith("<<END-PROXY>>")) {
              log.info("received stop signal in proxy, now stopping proxy.")
              disconnected = true;
              close(client, server, serverSocket)
              log.info("stopped proxy.")
            }
            first = true;
          }
          if (!disconnected) {
            streamToServer.write(request, 0, bytesRead)
            streamToServer.flush
          }
        }
        log.info("client request forwarded through proxy to server")
      }
    }
    catch {
      case e: IOException => {
        log.error(e, "IOException in network proxy reading from streamFromclient, writing to streamToServer.")
      }
    }
  }

  def streamServerResponsesToClient(client: Socket, server: Socket, streamToClient: OutputStream, streamFromServer: InputStream): Unit = {
    var bytesRead: Int = 0
    var reply: Array[Byte] = new Array[Byte](4096)
    try {
      log.info("writing response from server back to client")
      using(streamToClient, streamFromServer, client, server) {
        while (!disconnected && (({
          bytesRead = streamFromServer.read(reply);
          bytesRead
        })) != -1) {
          serverFunction(client, server, streamFromServer, streamToClient)
          log.info("in loop reading from remote server, writing back to client")
          streamToClient.write(reply, 0, bytesRead)
          streamToClient.flush
          log.info("in loop reading from remote server, written back to client")
        }
      }

      log.info("Finished streaming server responses back to client")
    }
    catch {
      case e: SocketException => {
        log.error(e, "SocketException in network proxy in the reading streamFromServer, writing to streamToClient.")
      }
      case e: IOException => {
        log.error(e, "IOException in network proxy in the reading streamFromServer, writing to streamToClient.")
      }
    }
  }

  def run: Unit = {
    disconnected = false
    serverSocket = new ServerSocket(localPort)
    stopBarrier.reset
    startBarrier.await
    log.info("proxy running on %s:%s", host, localPort)

    while (!disconnected) {
      try {
        val client = serverSocket.accept
        log.info("accept in proxy")
        val (server, streamFromClient, streamToClient, streamFromServer, streamToServer) = connectClientToRemoteServer(client)
        log.info("connected client and server through  proxy")
        spawn {streamClientRequestToServer(client, server, streamFromClient, streamToServer)}
        log.info("forwarded request to proxy, now spawning server responses to client")
        spawn {streamServerResponsesToClient(client, server, streamToClient, streamFromServer)}
      }
      catch {
        case e: IOException => {
          log.error(e, "Error in Proxy Run")
        }
      }
    }
    log.info("Proxy Thread has ended.")
    stopBarrier.await
  }

  def waitUntilStarted = {
    startBarrier.await
  }
  def stop = {
    startBarrier.reset
    try {
      log.info("connecting to stop the proxy on %s:%s", host, localPort)
      var client: Socket = new Socket(host, localPort)
      val streamToClient: OutputStream = client.getOutputStream
      val streamFromClient: InputStream = client.getInputStream
      using(streamToClient, streamFromClient, client) {
        log.info("writing stop message to proxy on %s:%s", host, localPort)
        streamToClient.write("<<END-PROXY>>".getBytes)
        streamToClient.flush
        log.info("written stop message to proxy on %s:%s", host, localPort)
        streamFromClient.read();
      }
      stopBarrier.await

    }
    catch {
      case e: Exception => {
        log.error(e, "Exception stopping proxy.")
      }
    }
  }
}