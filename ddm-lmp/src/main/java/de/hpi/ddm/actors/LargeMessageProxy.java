package de.hpi.ddm.actors;

import akka.NotUsed;
import akka.actor.*;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.configuration.ConfigurationSingleton;
import de.hpi.ddm.structures.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.jodah.expiringmap.ExpiringMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Constants      //
	////////////////////

	// This constant is used by the sender instance of the LargeMessageProxy, and defines the maximum amount of
	// messages that can be simultaneously available for transfer through the network (i.e. hosted on the HTTP server).
	// This limit is important because otherwise, a large influx of large messages can exhaust the available memory
	private static int MAX_LARGE_MESSAGES_IN_FLIGHT = 20;
	// This constant is used by the sender instance of the LargeMessageProxy, and defines the maximum number of seconds
	// that a large message is available for transfer through the network. Even though we expect to receive an
	// acknowledgement once a large message is successfully transferred, it could never arrive (e.g. if messages are lost).
	// This guarantees that in such case:
	//   (1) we don't cause a leak memory by storing a large message forever, and
	//   (2) we don't deadlock due to the maximum number of large messages in flight.
	private static int LARGE_MESSAGE_TIMEOUT_SECONDS = 30;

	////////////////////
	// Actor Messages //
	////////////////////

	// This message is sent by the parent of this actor to the sender LargeMessageProxy instance when it wishes
	// to transfer a large message to its counterpart receiver.
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> {
		private T message;
		private ActorRef receiver;
	}

	// This message is sent by the sender LargeMessageProxy instance to the received LargeMessageProxy instance
	// and notifies it that a large message is available for download (at the specified URL through HTTP).
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessageAvailableForDownload implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private String url;
		private ActorRef sender;
		private ActorRef receiver;
	}

	// This message is sent from the receiver LargeMessageProxy instance to the sender LargeMessageProxy instance
	// as an acknowledgement when a large message has been successfully sent and therefore can be deleted from the sender
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessageDownloaded implements Serializable {
		private static final long serialVersionUID = -2577152699714937534L;
		private String url;
	}

	// This message is generated internally by the sender LargeMessageProxy instance when a large message has been
	// available for download for a reasonable time, but no acknowledgement (LargeMessageDownloaded) has been received
	@Data @AllArgsConstructor
	public static class LargeMessageDownloadExpired {
		private String url;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	// Instance of an HTTP server that is used to host and transfer the messages between the sender and receiver LargeMessageProxy.
	private LargeMessageHttpServer httpServer;

	// Stores all URLs that are currently being hosted in the HTTP server, and manages expiration of large messages that
	// have been available a reasonable amount of time but never acknowledged.
	// REMARK: This instance is a Map, but we only use it like a Set - we don't care about the values in the map
	private ExpiringMap<String, String> inFlightMessages = ExpiringMap.builder()
			.expiration(LARGE_MESSAGE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
			.expirationListener((String url, String unused) -> {
				// This callback is called asynchronously, so it's not a good idea to manipulate the state of the class here
				// Instead, send a message to ourselves to return to the Akka world and avoid having to manage concurrency ourselves
				self().tell(new LargeMessageDownloadExpired(url), self());
			})
			.build();

	// Stores all LargeMessage instances that are waiting to be transferred,
	// due to there already being too many messages in flight (see MAX_LARGE_MESSAGES_IN_FLIGHT).
	private Queue<LargeMessage<?>> largeMessageQueue = new LinkedList<>();

	/////////////////
	// HTTP server //
	/////////////////

	private static class LargeMessageHttpServer extends AllDirectives {
		private final ActorSystem system;

		private String httpHost;
		private int httpPort;

		private CompletionStage<ServerBinding> httpServerBinding;

		// Map of routes along with associated file content. Note that this MUST be a concurrent hash map,
		// since if can be simultaneously accessed from both the HTTP server's createRoute() and
		// our actor's host()/unhost() calls
		private ConcurrentHashMap<String, byte[]> routeToContent = new ConcurrentHashMap<>();

		LargeMessageHttpServer(ActorSystem system) {
			system.log().info("[HTTPServer] constructor system=" + system.hashCode());

			this.system = system;

			// Determine the host and port that the HTTP server will use.
			// To make things simple, we pick the port of the HTTP server as the port of the Akka actor system plus 1.
			// This works because there is ever only one LargeMessageProxy transferring data
			// (the one associated to the Master actor), so there are no port conflicts.
			// If necessary in the future, a more robust strategy for picking the port should be used
			Configuration c = ConfigurationSingleton.get();
			httpHost = c.getHost();
			httpPort = c.getPort() + 1;

			final Http http = Http.get(system);
			final ActorMaterializer materializer = ActorMaterializer.create(system);

			final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = createRoute().flow(system, materializer);
			httpServerBinding = http.bindAndHandle(routeFlow, ConnectHttp.toHost(httpHost, httpPort), materializer);

			system.log().info("[HTTPServer] Server online at http://" + httpHost + ":" + httpPort + "/");
		}

		void stop() {
			system.log().info("[HTTPServer] stopping system=" + system.hashCode());
			if (routeToContent.size() > 0)
				system.log().warning("[HTTPServer] server still had " + routeToContent.size() + " (unacknowledged) hosted files!");

			httpServerBinding.thenCompose(ServerBinding::unbind); // stop the server and trigger unbinding from the port

			system.log().info("[HTTPServer] Server offline at http://" + httpHost + ":" + httpPort + "/");
		}

		// This is called every time that an HTTP request is made, and allows us to determine which URLs host which files.
		private Route createRoute() {
			return this.get(() ->
					routeToContent.entrySet().stream().map(fileEntry ->
							path(fileEntry.getKey(), () -> get(
									() -> complete(HttpResponse.create().withEntity(fileEntry.getValue()))
							))
					).reduce(reject(), Route::orElse)
			);
		}

		String host(byte[] contentBytes) {
			// Generate a random route for the content, save it along with the content in the route table, and return it
			String route = UUID.randomUUID().toString();
			String url = "http://" + httpHost + ":" + httpPort + "/" + route;
			system.log().info("[HTTPServer] Hosting content at " + url);
			routeToContent.put(route, contentBytes);
			return url;
		}

		void unhost(String url) {
			// Removes the content at the specified URL, releasing the associated resources
			system.log().info("[HTTPServer] Unhosting content at " + url);
			String route = url.substring(url.lastIndexOf("/") + 1);
			routeToContent.remove(route);
		}
	}

	/////////////////////
	// Utility methods //
	/////////////////////

	// Downloads the content available URL and returns it as a raw array of bytes
	private static byte[] download(String urlString) throws IOException {
		URL url = new URL(urlString);

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

		try (InputStream stream = url.openStream()) {
			byte[] chunk = new byte[4096];
			for (int bytesRead = stream.read(chunk); bytesRead > 0; bytesRead = stream.read(chunk)) {
				byteArrayOutputStream.write(chunk, 0, bytesRead);
			}
		}

		return byteArrayOutputStream.toByteArray();
	}
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////


	@Override
	public void postStop() throws Exception {
		super.postStop();

		// If this actor needed to create a HTTP server (sender LargeMessageProxy instance), release it now
		if (httpServer != null) {
			httpServer.stop();
			httpServer = null;
		}
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(LargeMessageAvailableForDownload.class, this::handle)
				.match(LargeMessageDownloaded.class, this::handle)
				.match(LargeMessageDownloadExpired.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		// Store the received message in the message queue. If possible, the message transfer will be started immediately,
		// otherwise, it will stay in the queue until it is possible to start the transfer
		largeMessageQueue.add(message);
		checkIfLargeMessageCanBeMadeAvailable();
	}

	private void checkIfLargeMessageCanBeMadeAvailable() {
		// Keep starting new message transfers until all messages are already being transferred,
		// or we reach the limit of messages in flights
		while (inFlightMessages.size() < MAX_LARGE_MESSAGES_IN_FLIGHT && largeMessageQueue.size() > 0) {
			try {
				LargeMessage<?> message = largeMessageQueue.remove();

				// Serialization with Kryo
				byte[] messageBytes = KryoPoolSingleton.get().toBytesWithClass(message.getMessage());

				// Make the message available through HTTP
				httpServer = (httpServer == null) ? new LargeMessageHttpServer(context().system()) : httpServer;
				String url = httpServer.host(messageBytes);
				log().info("[LargeMessageProxy] Message of length " + messageBytes.length + " hosted at " + url);

				// Notify the received instance of the LargeMessageProxy that a new message can be downloaded
				ActorRef receiver = message.getReceiver();
				ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
				receiverProxy.tell(new LargeMessageAvailableForDownload(url, this.sender(), message.getReceiver()), this.self());

				inFlightMessages.put(url, url);
			} catch (Exception e) {
				log().error(e, "[LargeMessageProxy] handle(LargeMessage)");
			}
		}
	}

	private void handle(LargeMessageAvailableForDownload message) {
		try {
			log().info("[LargeMessageProxy] Downloading message from " + message.getUrl());

			// Download the content of the message
			// TODO: This does blocking IO. If this is not acceptable, this should be made asynchronous
			byte[] bytes = download(message.getUrl());
			log().info("[LargeMessageProxy] Downloaded message with size=" + bytes.length);

			// Acknowledge that the message has been received to the sender LargeMessageProxy instance
			sender().tell(new LargeMessageDownloaded(message.getUrl()), self());

			// Deserialization with Kryo
			Object o = KryoPoolSingleton.get().fromBytes(bytes);
			log().info("[LargeMessageProxy] Message object " + o + " being delivered to " + message.getReceiver());

			// Finally, transfer the message to its final target
			message.getReceiver().tell(o, message.getSender());
		} catch (IOException e) {
			log().error(e, "[LargeMessageProxy] handle(BytesMessage)");
		}
	}

	private void handle(LargeMessageDownloaded message) {
		log().info("[LargeMessageProxy] BytesMessageDownloaded with url=" + message.getUrl());

		// Release the resources associated with the in-flight message
		inFlightMessages.remove(message.getUrl());
		if (httpServer != null) {
			httpServer.unhost(message.getUrl());
		}

		// Since we released a in-flight message slot, check if a further transfer can be started now
		checkIfLargeMessageCanBeMadeAvailable();
	}

	private void handle(LargeMessageDownloadExpired message) {
		log().info("[LargeMessageProxy] BytesMessageExpired with url=" + message.getUrl());

		// Release the resources associated with the in-flight message
		inFlightMessages.remove(message.getUrl());
		if (httpServer != null) {
			httpServer.unhost(message.getUrl());
		}

		// Since we released a in-flight message slot, check if a further transfer can be started now
		checkIfLargeMessageCanBeMadeAvailable();
	}
}
