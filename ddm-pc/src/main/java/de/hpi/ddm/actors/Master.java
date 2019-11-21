package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.structures.StudentRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	private Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workerPool = context().actorOf(WorkerPool.props(), WorkerPool.DEFAULT_NAME);
		this.workers = new ArrayList<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<StudentRecord> records;
	}

	@Data
	static class BatchCompleteMessage implements Serializable {
		private static final long serialVersionUID = 1235602981358319429L;
	}

	@Data @NoArgsConstructor
	static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final ActorRef workerPool;
	private final List<ActorRef> workers;

	private ActorRef batchProcessor;

	private long startTime;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(BatchCompleteMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();

		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	private void handle(BatchMessage message) {
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		if (message.getRecords().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			System.out.println("--------terminate--------");
			this.terminate();
			return;
		}

		// Create a new batch processor and forward the batch to it
		this.batchProcessor = context().actorOf(BatchProcessor.props(collector, workerPool));
		this.batchProcessor.tell(message, self());

		// this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		// this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	private void handle(BatchCompleteMessage message) {
		// Kill the batch processor that handled the current batch (we will create a new one)
		this.batchProcessor.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.batchProcessor = null;

		// Request more work from the reader
		this.reader.tell(new Reader.ReadMessage(), self());
	}

	private void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.workerPool.tell(PoisonPill.getInstance(), ActorRef.noSender());
		if (this.batchProcessor != null) {
			this.batchProcessor.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	private void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());

		workerPool.tell(new WorkerPool.NotifyWorkerAvailableMessage(sender()), self());

		this.log().info("Registered {}", this.sender());
	}
	
	private void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
	}
}
