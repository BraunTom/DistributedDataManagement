package de.hpi.ddm.actors;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.structures.SHA256Hash;
import de.hpi.ddm.structures.StudentRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
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
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<StudentRecord> records;
	}

	@Data @NoArgsConstructor
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @NoArgsConstructor
	public static class CanStart implements Serializable {
		private static final long serialVersionUID = -1225459087439901018L;
	}

	@Data @AllArgsConstructor @NoArgsConstructor
	public static class WorkItem implements Serializable {
		private StudentRecord record;
	}

	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;

	private LinkedList<WorkItem> workItems = new LinkedList<>();
	private LinkedList<ActorRef> idleWorkers = new LinkedList<>();

	private HashSet<Integer> assignedItemsIds = new HashSet<>();

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
				.match(Worker.RequestWork.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void handle(BatchMessage message) {
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		if (message.getRecords().isEmpty()) {
			if (this.assignedItemsIds.isEmpty()) {
				this.collector.tell(new Collector.PrintMessage(), this.self());
				System.out.println("--------terminate--------");
				this.terminate();
			}
			return;
		}

		for (StudentRecord line : message.getRecords()) {
			this.workItems.add(new WorkItem(line));
		}

		System.out.println(this.workItems.size());
		tryAssignWork();

		// this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		// this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	private void tryAssignWork() {
		while (!idleWorkers.isEmpty() && !workItems.isEmpty()) {
			WorkItem item = this.workItems.removeFirst();
			idleWorkers.remove().tell(item, self());
			this.assignedItemsIds.add(item.record.getId());
		}

		if (this.workItems.isEmpty()) {
			this.reader.tell(new Reader.ReadMessage(), self());
		}
	}
	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(Worker.RequestWork message) {
		idleWorkers.add(this.sender());

		if (message.containsResults()) {
			this.collector.tell(new Collector.CollectMessage(message.getResult()), self());
			this.assignedItemsIds.remove(message.getId());
		}

		tryAssignWork();
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());

		this.sender().tell(new CanStart(), self());

//		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}
}
