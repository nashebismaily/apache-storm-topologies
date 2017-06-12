package HBaseStreaming;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * HBaseStreamingTopology builds and runs the Storm Topology
 * 
 * @author Nasheb Ismaily
 */
public class HBaseStreamingTopology {
	
	public static void main(String[] args) throws Exception {
				
		//Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("hbase-spout",new HBaseStreamSpout());
		builder.setBolt("hbase-writer", new HBaseStreamBolt()).shuffleGrouping("hbase-spout");
		
		//Configuration object
		Config conf = new Config();
		
		//Topology Local run
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hbase-stream-topology",conf, builder.createTopology());
                Thread.sleep(100000);
                cluster.killTopology("hbase-stream-topology");
                cluster.shutdown();
        
		//Cluster Run
      //  StormSubmitter.submitTopology("hbase-stream-topology", conf, builder.createTopology());

	}
}


