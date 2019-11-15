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
		String[] line = workItem.getLine();
		Set<Character> possibleCharacters = new HashSet<>();


		for (String hint : workItem.hints()) {
			possibleCharacters.add(this.findUncommonCharacter(workItem.possibleCharacters(),
					this.findMatchingPermutation(workItem.possibleCharacters(), workItem.passwordLength(), hint)));
		}

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
	
	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes(StandardCharsets.UTF_8));
			
			StringBuilder stringBuffer = new StringBuilder();
			for (byte hashedByte : hashedBytes) {
				stringBuffer.append(Integer.toString((hashedByte & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	//////////////////////////////
	// helper from the internet //
	//////////////////////////////

	// https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
	// wrapper method for nicer access
	private char[] findMatchingPermutation(char[] set, int k, String hash) {
		int n = set.length;
		return this.findMatchingPermutationRec(set, "", n, k, hash).toCharArray();
	}

	// recursively generates permutations of length k until one is found that matches hash
	private String findMatchingPermutationRec(char[] set, String prefix, int n, int k, String hash) {
		if (k == 0) {
			return this.hash(prefix).equals(hash) ? prefix : null;
		}

		String result = null;

		for (int i = 0; i < n && result == null; ++i) {
			String newPrefix = prefix + set[i];
			result = this.findMatchingPermutationRec(set, newPrefix, n, k - 1, hash);
		}

		return result;
	}

	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, int n, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}
}