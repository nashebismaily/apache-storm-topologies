package StormSerialization;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Build and run the Storm Topology
 * 
 * @author Nasheb Ismaily
 */
public class KyroSerializationTopology {
	
	public static void main(String[] args) throws Exception {
				
		//Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-spout",new WordSpout());
		builder.setBolt("object-creator", new ObjectCreatorBolt()).shuffleGrouping("word-spout");
		builder.setBolt("object-reader", new ObjectReaderBolt()).shuffleGrouping("object-creator");
		
		//Configuration
		Config conf = new Config();
		//Set the Serializer/Deserializer for the User object
		conf.registerSerialization( User.class, TupleKyroSerializer.class);
		
		//Topology Local run
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("serialization-test", conf, builder.createTopology());
		Thread.sleep(100000);	// Tells the cluster how long to run the Topology
		cluster.killTopology("serialization-test"); // Stops the Topology 
		cluster.shutdown();	// Shuts down the local Storm cluster

	}
}


