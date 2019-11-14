package de.hpi.ddm.actors;

import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

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

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private String url;
		private ActorRef sender;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessageDownloaded implements Serializable {
		private static final long serialVersionUID = -2577152699714937534L;
		private String url;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private LargeMessageHttpServer httpServer;

	/////////////////
	// HTTP server //
	/////////////////

	private static class LargeMessageHttpServer extends AllDirectives {
		private final ActorSystem system;
		private String httpHost;
		private int httpPort;

		private CompletionStage<ServerBinding> binding;
		private HashMap<String, byte[]> files = new HashMap<>();

		LargeMessageHttpServer(ActorSystem system) {
			system.log().info("[HTTPServer] constructor system=" + system.hashCode());

			this.system = system;

			Configuration c = ConfigurationSingleton.get();
			httpHost = c.getHost();
			httpPort = c.getPort() + 1;

			final Http http = Http.get(system);
			final ActorMaterializer materializer = ActorMaterializer.create(system);

			final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = createRoute().flow(system, materializer);
			binding = http.bindAndHandle(routeFlow, ConnectHttp.toHost(httpHost, httpPort), materializer);

			system.log().info("[HTTPServer] Server online at http://" + httpHost + ":" + httpPort + "/");
		}

		public void stop() {
			system.log().info("[HTTPServer] stopping system=" + system.hashCode());
			if (files.size() > 0)
				system.log().warning("[HTTPServer] server still had " + files.size() + " unacknowledged files!");

			binding.thenCompose(ServerBinding::unbind); // trigger unbinding from the port

			system.log().info("[HTTPServer] Server offline at http://" + httpHost + ":" + httpPort + "/");
		}

		private Route createRoute() {
			return this.get(() ->
					files.entrySet().stream().map(fileEntry ->
							path(fileEntry.getKey(), () -> get(
									() -> complete(HttpResponse.create().withEntity(fileEntry.getValue()))
							))
					).reduce(reject(), Route::orElse)
			);
		}

		String host(byte[] messageBytes) {
			String uuid = UUID.randomUUID().toString();
			system.log().info("[HTTPServer] Host " + uuid);
			files.put(uuid, messageBytes);
			return "http://" + httpHost + ":" + httpPort + "/" + uuid;
		}

		void unhost(String url) {
			String uuid = url.substring(url.lastIndexOf("/") + 1);
			system.log().info("[HTTPServer] Unhost " + uuid);
			files.remove(uuid);
		}
	}

	/////////////////////
	// Utility methods //
	/////////////////////

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
	public void postStop() throws Exception, Exception {
		super.postStop();

		// If this actor needed to create a HTTP server, release it now
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
				.match(BytesMessage.class, this::handle)
				.match(BytesMessageDownloaded.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		
		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
		try {
			httpServer = (httpServer == null) ? new LargeMessageHttpServer(context().system()) : httpServer;
			byte[] messageBytes = KryoPoolSingleton.get().toBytesWithClass(message.getMessage());
			String url = httpServer.host(messageBytes);
			log().info("[LargeMessageProxy] Message of length " + messageBytes.length + " hosted at " + url);
			receiverProxy.tell(new BytesMessage(url, this.sender(), message.getReceiver()), this.self());
		} catch (Exception e) {
			log().error(e, "[LargeMessageProxy] handle(LargeMessage)");
		}
	}

	private void handle(BytesMessage message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		try {
			log().info("[LargeMessageProxy] Downloading message from " + message.getUrl());

			byte[] bytes = download(message.getUrl());
			sender().tell(new BytesMessageDownloaded(message.getUrl()), self());

			log().info("[LargeMessageProxy] Downloading message of " + bytes.length);

			Object o = KryoPoolSingleton.get().fromBytes(bytes);
			log().info("[LargeMessageProxy] Message object " + o + " being delivered to " + message.getReceiver());

			message.getReceiver().tell(o, message.getSender());
		} catch (IOException e) {
			log().error(e, "[LargeMessageProxy] handle(BytesMessage)");
		}
	}

	private void handle(BytesMessageDownloaded message) {
		log().info("[LargeMessageProxy] BytesMessageDownloaded with url=" + message.getUrl());
		if (httpServer != null) {
			httpServer.unhost(message.getUrl());
		}
	}
}
