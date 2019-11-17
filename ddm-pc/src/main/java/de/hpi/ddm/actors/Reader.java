package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import com.opencsv.CSVReader;
import de.hpi.ddm.configuration.ConfigurationSingleton;
import de.hpi.ddm.configuration.DatasetDescriptorSingleton;
import de.hpi.ddm.structures.SHA256Hash;
import de.hpi.ddm.structures.StudentRecord;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Reader extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "reader";

	public static Props props() {
		return Props.create(Reader.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class ReadMessage implements Serializable {
		private static final long serialVersionUID = -3254147511955012292L;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private CSVReader reader;
	
	private int bufferSize;
	
	private List<StudentRecord> buffer;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() throws Exception {
		Reaper.watchWithDefaultReaper(this);
		
		this.reader = DatasetDescriptorSingleton.get().createCSVReader();
		this.bufferSize = ConfigurationSingleton.get().getBufferSize();
		this.buffer = new ArrayList<>(this.bufferSize);
		
		this.read();
	}

	@Override
	public void postStop() throws Exception {
		this.reader.close();
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ReadMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(ReadMessage message) throws Exception {
		this.sender().tell(new Master.BatchMessage(new ArrayList<>(this.buffer)), this.self());
		
		this.read();
	}
	
	private void read() throws Exception {
		this.buffer.clear();
		
		String[] line;
		while ((this.buffer.size() < this.bufferSize) && ((line = this.reader.readNext()) != null))
			this.buffer.add(parseStudentRecord(line));
	}

	private StudentRecord parseStudentRecord(String[] line) {
		if (line.length < 5) {
			throw new IllegalArgumentException("A student record line must have at least 5 fields.");
		}

		return new StudentRecord(
				Integer.parseInt(line[0]),
				line[1],
				line[2],
				Integer.parseInt(line[3]),
				SHA256Hash.fromHexString(line[4]),
				Arrays.stream(line, 5, line.length).map(SHA256Hash::fromHexString).toArray(SHA256Hash[]::new)
		);
	}
}
