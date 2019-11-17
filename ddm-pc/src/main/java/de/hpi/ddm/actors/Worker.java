package de.hpi.ddm.actors;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

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
import lombok.Data;
import lombok.NoArgsConstructor;

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
		private static final long serialVersionUID = 0;
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

	private char findUncommonCharacter(char[] allChars, char[] hints) throws Exception {
		Set<Character> match = new HashSet<>();

		for (char hint : hints) {
			match.add(hint);
		}

		for (char cha : allChars) {
			if (match.contains(cha)) {
				return cha;
			}
		}

		throw new Exception("WTF");
	}

	private void handle(Master.WorkItem workItem) throws Exception {
		log().info("[Worker] Handling WorkItem");

		HashMap<SHA256Hash, AbstractMap.SimpleImmutableEntry<String, Character>> hintsToCrack = new HashMap<>();
		for (String hint : workItem.hints()) {
			SHA256Hash hintHash = new SHA256Hash();
			hintHash.fromHexString(hint);

			hintsToCrack.put(hintHash, new AbstractMap.SimpleImmutableEntry<>("", '\0'));
		}

		log().info("[Worker] hintsToCrack: " + hintsToCrack.size() + " /// " + Arrays.toString(workItem.possibleCharacters()));

		this.crackPasswordHints(workItem.possibleCharacters(), hintsToCrack);

		Set<Character> possibleCharacters = new HashSet<>();
		for (Map.Entry<SHA256Hash, AbstractMap.SimpleImmutableEntry<String, Character>> entry : hintsToCrack.entrySet()) {
			possibleCharacters.add(entry.getValue().getValue());
		}

		log().info("[Worker] PossibleCharacters: " + Arrays.toString(possibleCharacters.toArray(new Character[0])));

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

	// Same algorithm as hashcat-utils permute.c
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

	private static void tryCrackPasswordHint(byte[] candidate, HashMap<SHA256Hash, AbstractMap.SimpleImmutableEntry<String, Character>> hintsToCrack) throws Exception {
		SHA256Hash candidateHash = new SHA256Hash();
		candidateHash.fromData(candidate, candidate.length - 1); // Ignore last character (not in hint, just used for the permutations!)

		if (hintsToCrack.containsKey(candidateHash)) {
			hintsToCrack.replace(candidateHash, new AbstractMap.SimpleImmutableEntry<>(
					new String(candidate, 0, candidate.length - 1, StandardCharsets.US_ASCII),
					(char) candidate[candidate.length - 1]
			));
		}
	}

	private void crackPasswordHints(byte[] passwordChars, HashMap<SHA256Hash, AbstractMap.SimpleImmutableEntry<String, Character>> hintsToCrack) throws Exception {
		byte[] permutation = passwordChars.clone();

		int[] p = new int[passwordChars.length + 1];
		for (int k = 0; k < p.length; k++)
			p[k] = k;

		int k = 1;
		tryCrackPasswordHint(permutation, hintsToCrack);

		while ((k = getNextPermutation(permutation, p, k)) != passwordChars.length) {
			tryCrackPasswordHint(permutation, hintsToCrack);
		}

		tryCrackPasswordHint(permutation, hintsToCrack);
	}
}