package WordCount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Build and run the Storm Topology
 * 
 * @author Nasheb Ismaily
 */
public class WordCountTopology {
	
	public static void main(String[] args) throws Exception {
		
		// The text file that will be processed
		String textFile = "/path/to/words/file.txt";
				
		//Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader",new WordReaderSpout());
		builder.setBolt("word-normalizer", new WordNormalizerBolt()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounterBolt()).fieldsGrouping("word-normalizer", new Fields("word"));
		
		//Configuration object
		Config conf = new Config();
		conf.setDebug(false);
		conf.put("wordsFile", textFile);
		
		//Topology Local run
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("word-count-topology",conf, builder.createTopology());
        Thread.sleep(100000);
        cluster.killTopology("word-count-topology");
        cluster.shutdown();
	}
}


