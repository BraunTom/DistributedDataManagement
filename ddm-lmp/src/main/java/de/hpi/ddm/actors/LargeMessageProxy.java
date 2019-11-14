package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import akka.serialization.Serializers;
import akka.stream.ActorMaterializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.sql.DataSource;

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
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	@Data @AllArgsConstructor
	static class SourceOffer {
		final SourceRef<SerializedMessage> sourceRef;
		private ActorRef sender;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor
	static class SerializedMessage {
		private byte[] bytes;
		private int serializerId;
		private String manifest;

		SerializedMessage(LargeMessage<?> largeMessage, Serialization serialization) {
			this.bytes = serialization.serialize(largeMessage).get();
			this.serializerId = serialization.findSerializerFor(largeMessage).identifier();
			this.manifest = Serializers.manifestFor(serialization.findSerializerFor(largeMessage), largeMessage);
		}

		LargeMessage<?> deserialize(Serialization serialization) {
			return (LargeMessage<?>) serialization.deserialize(this.bytes, this.serializerId, this.manifest).get();
		}
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(SourceOffer.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private Serialization serialization() {
		return SerializationExtension.get(this.context().system());
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		Source<SerializedMessage, NotUsed> s = Source.single(new SerializedMessage(message, this.serialization()));
		CompletionStage<SourceRef<SerializedMessage>> refs = s.runWith(StreamRefs.sourceRef(), ActorMaterializer.create(this.context().system()));

		Patterns.pipe(refs.thenApply(ref -> new SourceOffer(ref, this.sender(), message.getReceiver())), this.context().dispatcher())
				.to(receiverProxy);
	}

	private void handle(SourceOffer sourceOffer) {
		sourceOffer.sourceRef.getSource().runWith(
				Sink.foreach(x -> sourceOffer.receiver.tell(x.deserialize(this.serialization()).message, sourceOffer.sender)),
				ActorMaterializer.create(this.getContext().getSystem()));
	}
}
