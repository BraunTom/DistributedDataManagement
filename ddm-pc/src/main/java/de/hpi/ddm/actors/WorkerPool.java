package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Manages assignment of a set of work items (messages) over a set of workers.
 * This is similar to an Akka Router, but tuned to our use case where workers can be added after work has already started.
 */
public class WorkerPool extends AbstractLoggingActor {
    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "workerpool";

    public static Props props() {
        return Props.create(WorkerPool.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data @NoArgsConstructor @AllArgsConstructor
    static class NotifyWorkerAvailableMessage implements Serializable {
        private static final long serialVersionUID = -50374816448627600L;

        private ActorRef worker;
    }

    /////////////////
    // Actor State //
    /////////////////

    private LinkedList<WorkItem> workItems = new LinkedList<>();
    private LinkedList<ActorRef> idleWorkers = new LinkedList<>();

    @Value @AllArgsConstructor
    private static class WorkItem {
        private final Object message;
        private final ActorRef sender;
    }

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
                .match(NotifyWorkerAvailableMessage.class, this::handle)
                .matchAny(this::handle)
                .build();
    }

    private void handle(NotifyWorkerAvailableMessage message) {
        idleWorkers.add(message.getWorker());
        tryAssignWork();
    }

    private void handle(Object message) {
        workItems.add(new WorkItem(message, sender()));
        tryAssignWork();
    }

    private void tryAssignWork() {
        while (!idleWorkers.isEmpty() && !workItems.isEmpty()) {
            WorkItem item = workItems.removeFirst();
            ActorRef worker = idleWorkers.removeFirst();

            worker.tell(item.getMessage(), item.getSender());
        }
    }
}
