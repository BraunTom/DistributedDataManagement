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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

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

	@Data @AllArgsConstructor @NoArgsConstructor
	static class CrackHintsBatchWorkItem implements Serializable {
		private static final long serialVersionUID = -416499721297254929L;

		private Set<SHA256Hash> hintHashes;
		private String prefix;
		private String choices;
	}

	@Data @AllArgsConstructor @NoArgsConstructor
	static class CrackFullPasswordWorkItem implements Serializable {
		private static final long serialVersionUID = -7708112313610425523L;

		private SHA256Hash fullPasswordHash;
		private String fullPasswordChars;
		private int fullPasswordLength;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	static class CrackedHintsMessage implements Serializable {
		private static final long serialVersionUID = 206938995923980746L;
		private Map<SHA256Hash, Character> crackedHints;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	static class CrackedFullPasswordMessage implements Serializable {
		private static final long serialVersionUID = -4803956548142547242L;
		private SHA256Hash fullPasswordHash;
		private String fullPassword;
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
				.match(Worker.CrackHintsBatchWorkItem.class, this::handle)
				.match(Worker.CrackFullPasswordWorkItem.class, this::handle)
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

	private void handle(Worker.CrackHintsBatchWorkItem workItem) {
		log().info("[Worker] Trying to crack {} hint hashes, starting with choices {} and prefix {}",
				workItem.getHintHashes().size(), workItem.getChoices(), workItem.getPrefix());

		// Crack the hints in this range
		Map<SHA256Hash, Character> crackedHints =
				new HintPermutationCracker(workItem.getChoices(), workItem.getPrefix()).crack(workItem.getHintHashes());

		log().info("[Worker] Cracked {} hint hashes, starting with choices {} and prefix {}",
				crackedHints.size(), workItem.getChoices(), workItem.getPrefix());

		// Send back the result (implicitly also asks for more work)
		sender().tell(new CrackedHintsMessage(crackedHints), self());
	}

	private void handle(Worker.CrackFullPasswordWorkItem message) {
		log().info("[Worker] Possible characters for the full password are: ({})", message.getFullPasswordChars());

		// Crack the full password
		String fullPassword = new CombinationCracker(message.getFullPasswordChars(), message.getFullPasswordLength()).crack(message.getFullPasswordHash());

		log().info("[Worker] Full password cracked: ({})", fullPassword);

		// Send back the result (implicitly also asks for more work)
		sender().tell(new CrackedFullPasswordMessage(message.getFullPasswordHash(), fullPassword), self());
	}
}