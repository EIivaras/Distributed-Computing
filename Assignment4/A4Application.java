import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;


public class A4Application {

    public static void main(String[] args) throws Exception {
		// do not modify the structure of the command line
		String bootstrapServers = args[0];
		String appName = args[1];
		String studentTopic = args[2];
		String classroomTopic = args[3];
		String outputTopic = args[4];
		String stateStoreDir = args[5];

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

		// add code here if you need any additional configuration options

		StreamsBuilder builder = new StreamsBuilder();

		// IDEA:
		// Kafka operates similar to spark, in terms of how you put together your code (one long line)
		// Drawing from word count, the idea would be to form a KTable based on the the lines of data that have already been read from the topic
		// Then, we reduce/group/perform operations on that table much like how we did with spark
		// However, because the input comes in a stream, every single time we have a new line of input, the table's values are re-calculated
		// 		--> Don't worry though, b/c all previous values will remain the same: only new lines will be added, previous lines will not be changed
		// By converting the KTable to a stream before writing to the output topic, we ensure that only the changes between tables are published to the output topic
		// Therefore, we operate in a stream!

		// Note: 
		// --> studentLines = a bunch of KV pairs of the form "Student_ID,Room_ID", which is an event log of students walking between rooms
		// --> classroomLines = a bunch of KV pairs of the form "Room_ID,Capacity", which is an event log of each room's capacity, which can change dynamically
		
		// Note: As in the lecture notes, if we "count" or something similar, make sure we do the counting in a stateful store
		// Example does it like so:
		// --> .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));

		// The real question is -- how do we manage with 2 streams? Stitching it together?
		// Looks like we can join KStreams together? Maybe this is an approach we want to take?
		// --> https://kafka.apache.org/21/javadoc/org/apache/kafka/streams/kstream/KStream.html

		KStream<String, String> studentLines = builder.stream(studentTopic);
		KTable<String, Long> roomsOccupancy = studentLines
												.groupByKey()
												// TO-DO: confirm reduce actually returns the latest value (is order preserved?)
												.reduce((oldValue, newValue) -> newValue)
												.groupBy((studentID, roomID) -> new KeyValue<String, String>(roomID, studentID))
												.count();								

		// KStream<String, String> classroomLines = builder.stream(classroomTopic);
		// classroomLines
		// 	.leftJoin(roomsOccupancy, (occupancy, capacity) -> occupancy - Long.parseLong(capacity))
		// 	.groupByKey()
		// 	.reduce((oldValue, newValue) -> {
		// 		if (newValue > 0 && newValue > oldValue) {

		// 		}
		// 	})

		// TODO:
		// 1. Convert streams to tables (in a table, each key is only used once (previous records with a given key are deleted))
		// 2. Left join tables on the student topic for the roomID (since not all rooms have a listed capacity)
		// 3. Group by roomID and statefully store the count of students in each room
		// 4. If the number is > than the listed capacity, provide output
		// 5. If the previous count of students was > and the incoming is <=, provide "OK" output
		// 		--> Maybe have a global KTable or something that lets us do this comparison
		// Note: In the lecture notes, to convert from KStream to KTable you generally go: KStream -> KGroupedStream -> KTable, so might need to group before the join?

		// classroomsCapacity
		// 	.leftJoin(roomsOccupancy, (occupancy, capacity) -> occupancy - Integer.parseInt(capacity))
		// 	.
			

			// .reduce(new Reducer<Long>() {
			// 	public Long countOccupants(Long newCount, Long oldCount) {
			// 		if (newCount < oldCount) {
			// 			System.out.println("A student left a class");
			// 		}
			// 		return newCount;
			// 	}
			// })

		// ...
		// ...to(outputTopic);

		KafkaStreams streams = new KafkaStreams(builder.build(), props);

		// this line initiates processing
		streams.start();

		// shutdown hook for Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
