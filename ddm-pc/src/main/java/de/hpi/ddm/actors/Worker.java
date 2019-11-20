package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import de.hpi.ddm.algorithms.CombinationCracker;
import de.hpi.ddm.algorithms.HintPermutationCracker;
import de.hpi.ddm.structures.SHA256Hash;
import de.hpi.ddm.structures.StudentRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.*;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor
	public static class RequestWork implements Serializable {
		private static final long serialVersionUID = -5534456615984629798L;
		private String result = null;
		private int id = -1;

		RequestWork(String result, int id) {
			this.result = result;
			this.id = id;
		}

		public boolean containsResults() {
			return this.result != null;
		}
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(Master.WorkItem.class, this::handle)
				.match(Master.CanStart.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private String findUncommonCharacters(String passwordCharacters, Character[] hintCharacters) throws Exception {
		Set<Character> match = new HashSet<>();
		for (char c : passwordCharacters.toCharArray()) {
			match.add(c);
		}
		for (char c : hintCharacters) {
		    match.remove(c);
		}
		return String.valueOf(ArrayUtils.toPrimitive(match.toArray(new Character[0])));
	}

	private void handle(Master.WorkItem workItem) throws Exception {
		StudentRecord record = workItem.getRecord();

		log().info("[Worker rid={}] Received new WorkItem", record.getId());

		// Crack the hint hashes
		log().info("[Worker rid={}] Cracking hint hashes", record.getId());

		Map<SHA256Hash, HintPermutationCracker.CrackedHint> crackedHints =
				new HintPermutationCracker(record.getPasswordChars()).crack(record.getHintHashes());

		log().info("[Worker rid={}] All hint hashes cracked ({})", record.getId(),
				Arrays.toString(crackedHints.values().stream().map(HintPermutationCracker.CrackedHint::getPlainText).toArray(String[]::new)));

		// Figure out the characters that can be in the password
		String fullPasswordCharacters = findUncommonCharacters(record.getPasswordChars(),
                crackedHints.values().stream().map(HintPermutationCracker.CrackedHint::getMissingCharacter).toArray(Character[]::new));

		log().info("[Worker rid={}] Possible characters for the full password are: ({})", record.getId(), fullPasswordCharacters);

		// Crack the full password
		String fullPassword = new CombinationCracker(fullPasswordCharacters, record.getPasswordLength()).crack(record.getFullPasswordHash());

		log().info("[Worker rid={}] Full password cracked: ({})", record.getId(), fullPassword);

		// Finished, request more work!
		sender().tell(new RequestWork(fullPassword, record.getId()), self());
	}

	private void handle(Master.CanStart canStart) {
		sender().tell(new RequestWork(), self());
	}
}