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
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueTransformer;

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


		// TODO:
		// 1. Convert streams to tables (in a table, each key is only used once (previous records with a given key are deleted))
		// 2. Left join tables on the student topic for the roomID (since not all rooms have a listed capacity)
		// 3. Group by roomID and statefully store the count of students in each room
		// 4. If the number is > than the listed capacity, provide output
		// 5. If the previous count of students was > and the incoming is <=, provide "OK" output
		// 		--> Maybe have a global KTable or something that lets us do this comparison
		// Note: In the lecture notes, to convert from KStream to KTable you generally go: KStream -> KGroupedStream -> KTable, so might need to group before the join?
		
		// Note: As in the lecture notes, if we "count" or something similar, make sure we do the counting in a stateful store
		// Example does it like so:
		// --> .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));

		KStream<String, String> studentLines = builder.stream(studentTopic);
		KTable<String, String> studentRooms = studentLines
												.groupByKey()
												// TODO: confirm reduce actually returns the latest value (is order preserved?)
												// a KTable that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key
												.reduce((oldValue, newValue) -> newValue);
		KTable<String, Long> roomsOccupancy = studentRooms
												.groupBy((studentID, roomID) -> new KeyValue<String, String>(roomID, studentID))
												.count();
		
												
		KStream<String, String> classroomLines = builder.stream(classroomTopic);

		KTable<String, Long> roomsCapacity = classroomLines
												.map((key, value) -> KeyValue.pair(key, Long.parseLong(value)))
												.groupByKey(Serialized.with(Serdes.String(), Serdes.Long()))
												.reduce((oldValue, newValue) -> newValue);

		KTable <String, String> joined = roomsOccupancy.leftJoin(roomsCapacity,
			(leftValue, rightValue)	-> { 
				if (leftValue != null && rightValue != null) {
					if (leftValue > rightValue) { 
						// Occupancy is greater than capacity
						return String.valueOf(leftValue); 
					} else if (leftValue <= rightValue) { 
						// classRoom is full
						return "PreOK"; 
					} 
				}
				return "";
			}
		);

		joined.toStream()
			  .groupByKey()
			  .reduce((oldValue, newValue) -> {
				  if (newValue.equals("PreOK") && (oldValue.equals("") || oldValue.equals("PreOK") || oldValue.equals("OK"))) {
					  // previously, occupancy was less than capacity
					  // now, occupancy is equal to capacity (classRoom is full, but we don't care)
					  return oldValue;
				  } else if (newValue.equals("PreOK")) {
					  return "OK";
				  }
				  return newValue;
			  })
			  .toStream()
			  .filter((key, value) -> !value.equals("") && !value.equals("PreOK"))
			  .to(outputTopic);

		KafkaStreams streams = new KafkaStreams(builder.build(), props);

		// this line initiates processing
		streams.start();

		// shutdown hook for Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}