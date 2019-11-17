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
import de.hpi.ddm.structures.SHA256Hash;
import de.hpi.ddm.structures.StudentRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
	}

	@Value @AllArgsConstructor
	private static class CrackedHint {
		String plainText;
		char missingCharacter;
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

	private char[] findUncommonCharacters(String passwordCharacters, Character[] hintCharacters) throws Exception {
		Set<Character> match = new HashSet<>();
		for (char c : passwordCharacters.toCharArray()) {
			match.add(c);
		}
		for (char c : hintCharacters) {
		    match.remove(c);
		}
		return ArrayUtils.toPrimitive(match.toArray(new Character[0]));
	}

	private void handle(Master.WorkItem workItem) throws Exception {
		StudentRecord record = workItem.getRecord();

		log().info("[Worker rid={}] Received new WorkItem", record.getId());

		// Crack the hint hashes
		log().info("[Worker rid={}] Cracking hint hashes", record.getId());
		HashMap<SHA256Hash, CrackedHint> hintsToCrack = new HashMap<>();
		for (SHA256Hash hintHash : record.getHintHashes()) {
			hintsToCrack.put(hintHash, null);
		}
		byte[] passwordCharsBytes = record.getPasswordChars().getBytes(StandardCharsets.UTF_8);
		this.crackPasswordHints(passwordCharsBytes, hintsToCrack);

		if (hintsToCrack.containsValue(null)) {
			throw new RuntimeException("Some hint hash could not be cracked!");
		}

		log().info("[Worker rid={}] All hint hashes cracked ({})", record.getId(),
				Arrays.toString(hintsToCrack.values().stream().map(CrackedHint::getPlainText).toArray(String[]::new)));

		// Figure out the characters that can be in the password
		char[] fullPasswordCharacters = findUncommonCharacters(record.getPasswordChars(),
                hintsToCrack.values().stream().map(CrackedHint::getMissingCharacter).toArray(Character[]::new));

		log().info("[Worker rid={}] Possible characters for the full password are: ({})", record.getId(),
			Arrays.toString(fullPasswordCharacters));

		/*ArrayList<String> possiblePasswords = this.allKLength(possibleCharacters.toString().toCharArray(), passwordLength);

		for (String possiblePassword : possiblePasswords) {
			if (this.hash(possiblePassword).equals(line[4])) {
				System.out.println(possiblePassword);
				return;
			}
		}*/
	}

	private void handle(Master.CanStart canStart) {
		sender().tell(new RequestWork(), self());
	}

	//////////////////////////////
	// helper from the internet //
	//////////////////////////////

	// Generates a new permutation in place using the "Countdown QuickPerm Algorithm" developed by Phillip Paul Fuchs
	// This is the algorithm used in hashcat-utils permute.c
	// See: https://github.com/hashcat/hashcat-utils/blob/f2a86c76c7ce38ebfeb6ea4a16b5dacd6c942afe/src/permute.c
	private static int getNextPermutation(byte[] word, int[] p, int k) {
		p[k]--;

		int j = (k % 2) * p[k];

		byte tmp = word[j];
		word[j] = word[k];
		word[k] = tmp;

		for (k = 1; p[k] == 0; k++)
			p[k] = k;

		return k;
	}

	private static void tryCrackPasswordHint(byte[] candidate, Map<SHA256Hash, CrackedHint> hintsToCrack) throws Exception {
		SHA256Hash candidateHash = SHA256Hash.fromDataHash(candidate, candidate.length - 1); // Ignore last character (not in hint, just used for the permutations!)

		if (hintsToCrack.containsKey(candidateHash)) {
			hintsToCrack.replace(candidateHash, new CrackedHint(
					new String(candidate, 0, candidate.length - 1, StandardCharsets.UTF_8),
					(char) candidate[candidate.length - 1]
			));
		}
	}

	private void crackPasswordHints(byte[] passwordChars, Map<SHA256Hash, CrackedHint> hintsToCrack) throws Exception {
		int[] p = new int[passwordChars.length + 1];
		for (int k = 0; k < p.length; k++)
			p[k] = k;

		int k = 1;
		tryCrackPasswordHint(passwordChars, hintsToCrack);

		while ((k = getNextPermutation(passwordChars, p, k)) != passwordChars.length) {
			tryCrackPasswordHint(passwordChars, hintsToCrack);
		}

		tryCrackPasswordHint(passwordChars, hintsToCrack);
	}
}