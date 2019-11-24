package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.ddm.structures.SHA256Hash;
import de.hpi.ddm.structures.StudentRecord;
import lombok.Data;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles the distribution of the work in a batch among multiple workers,
 * and the aggregation of the results received from them.
 */
public class BatchProcessor extends AbstractLoggingActor {
    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "batchprocessor";

    public static Props props(ActorRef collector, ActorRef workerPool) {
        return Props.create(BatchProcessor.class, () -> new BatchProcessor(collector, workerPool));
    }

    private BatchProcessor(ActorRef collector, ActorRef workerPool) {
        this.collector = collector;
        this.workerPool = workerPool;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef collector;
    private final ActorRef workerPool;

    /**
     * Stores the information associated with a student record,
     * along with the information about the cracking process that has been aggregated so far for it.
     */
    @Data
    private static class StudentCrackingWorkLog {
        StudentRecord record;
        Set<Character> potentialPasswordCharacters;
        int hintsRemainingToCrack;

        StudentCrackingWorkLog(StudentRecord record) {
            this.record = record;
            this.potentialPasswordCharacters = new HashSet<>();
            for (int i = 0; i < record.getPasswordChars().length(); i++)
                this.potentialPasswordCharacters.add(record.getPasswordChars().charAt(i));
            this.hintsRemainingToCrack = record.getHintHashes().length;
        }
    }

    // Those maps allow us to associate hint and full password hashes with the corresponding student work log,
    // in order to aggregate the results efficiently when they are received from the workers
    private MultiValuedMap<SHA256Hash, StudentCrackingWorkLog> hintHashToRegistry;
    private MultiValuedMap<SHA256Hash, StudentCrackingWorkLog> fullPasswordHashToRegistry;

    private int pendingHintMessages;

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
                .match(Master.BatchMessage.class, this::handle)
                .match(Worker.CrackedHintsMessage.class, this::handle)
                .match(Worker.CrackedFullPasswordMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(Master.BatchMessage message) {
        // Store the records in the batch in a way that is convenient for aggregating the results later
        this.fullPasswordHashToRegistry = new HashSetValuedHashMap<>();
        this.hintHashToRegistry = new HashSetValuedHashMap<>();
        for (StudentRecord r : message.getRecords()) {
            StudentCrackingWorkLog reg = new StudentCrackingWorkLog(r);
            for (SHA256Hash hintHash : r.getHintHashes()) {
                hintHashToRegistry.put(hintHash, reg);
            }
            fullPasswordHashToRegistry.put(r.getFullPasswordHash(), reg);
        }

        // Do some validations on the records in the batch. Those are the things we assume to be true, in
        // order to efficiently distribute the workload among our workers
        if (message.getRecords().stream().map(StudentRecord::getPasswordChars).distinct().count() != 1 ||
            message.getRecords().stream().mapToInt(StudentRecord::getPasswordLength).distinct().count() != 1) {
            throw new IllegalArgumentException("Expected all records in a batch to have the same password characters and length");
        }

        String passwordChars = message.getRecords().get(0).getPasswordChars();
        if (passwordChars.length() <= 3) {
            throw new IllegalArgumentException("Expected the password characters field to have at least 3 character");
        }

        // Distribute the work among workers, so that all hints in the batch are simultaneously cracked.
        // Each worker will iterate over all permutations of the potential hint plain texts, with a 2-character prefix
        Set<SHA256Hash> allBatchHintHashes = message.getRecords().stream()
                .flatMap(r -> Arrays.stream(r.getHintHashes()))
                .collect(Collectors.toSet());

        for (int i = 0; i < passwordChars.length(); i++) {
            for (int j = 0; j < passwordChars.length(); j++) {
                if (i != j) {
                    String prefix = String.valueOf(passwordChars.charAt(i)) + passwordChars.charAt(j);

                    String choices = new StringBuilder(passwordChars)
                            .deleteCharAt(Math.max(i, j)) // Remove the latest character first,
                            .deleteCharAt(Math.min(i, j)) // so the earliest character doesn't shift indices
                            .toString();

                    workerPool.tell(new Worker.CrackHintsBatchWorkItem(allBatchHintHashes, prefix, choices), self());
                }
            }
        }

        pendingHintMessages = passwordChars.length() * (passwordChars.length() - 1);
    }

    private void handle(Worker.CrackedHintsMessage message) {
        // Tell the worker pool that this worker can now pull more work
        workerPool.tell(new WorkerPool.NotifyWorkerAvailableMessage(sender()), self());

        pendingHintMessages--;

        log().info("[BatchProcessor] Aggregating {} cracked hints", message.getCrackedHints().size());

        // Look up which student's password hints have been cracked
        for (Map.Entry<SHA256Hash, Character> entry : message.getCrackedHints().entrySet()) {
            for (StudentCrackingWorkLog workLog : hintHashToRegistry.remove(entry.getKey())) {
                // Aggregate the results of the hint cracking process
                workLog.potentialPasswordCharacters.remove(entry.getValue());
                workLog.hintsRemainingToCrack--;

                // If all hints have been cracked, start cracking the full password!
                if (workLog.hintsRemainingToCrack == 0) {
                    log().info("[BatchProcessor] All hints for user with ID={} (NAME={}) cracked, starting password cracking",
                            workLog.getRecord().getId(), workLog.getRecord().getName());

                    StringBuilder potentialPasswordCharacters = new StringBuilder();
                    for (Character c : workLog.getPotentialPasswordCharacters())
                        potentialPasswordCharacters.append(c);

                    workerPool.tell(new Worker.CrackFullPasswordWorkItem(workLog.getRecord().getFullPasswordHash(),
                                    potentialPasswordCharacters.toString(), workLog.getRecord().getPasswordLength()), self());
                }
            }
        }

        if (fullPasswordHashToRegistry.isEmpty() && pendingHintMessages == 0) {
            context().parent().tell(new Master.BatchCompleteMessage(), self());
        }
    }

    private void handle(Worker.CrackedFullPasswordMessage message) {
        // Tell the worker pool that this worker can now pull more work
        workerPool.tell(new WorkerPool.NotifyWorkerAvailableMessage(sender()), self());

        log().info("[BatchProcessor] Received a cracked full password");

        // Look up which student's password have been cracked
        for (StudentCrackingWorkLog workLog : fullPasswordHashToRegistry.remove(message.getFullPasswordHash())) {
            collector.tell(new Collector.CollectMessage(String.format(
                    "The password of ID=%d (NAME=%s) is %s",
                    workLog.getRecord().getId(),
                    workLog.getRecord().getName(),
                    message.getFullPassword())), self());
        }

        // When all password have been cracked, tell the Master that the batch is finished
        if (fullPasswordHashToRegistry.isEmpty() && pendingHintMessages == 0) {
            context().parent().tell(new Master.BatchCompleteMessage(), self());
        }
    }


}
