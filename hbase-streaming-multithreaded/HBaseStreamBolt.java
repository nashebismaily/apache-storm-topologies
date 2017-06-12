package HBaseStreaming;

import java.util.Map;

import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.Config;

/**
 * HBaseStreamBolt will initialize multiple threads each containing a streamer which is represented by a LinkedBlockingQueue.
 * The incoming tuple is emitted to a queue for HBase table processing.
 * A time specific tick tuple will flush the streamers to HBase.
 * 
 * @author Nasheb Ismaily
 *
 */
public class HBaseStreamBolt extends BaseRichBolt {

	private static final String ZOOKEEPER_QUORUM = "192.168.56.123"; // Comma separated list of Hostname's or IP's'
	private static final String ZOOKEEPER_PORT = "2181"; // Zookeeper port
	private static final String ZOOKEEPER_NODE = "/hbase-unsecure"; // Hortonworks Data Platform - Zookeeper Node
	private static final String HBASE_WRITE_BUFFER = "4194304"; // Size of HBase table puts to batch before RPC call to HBase.
	private static final int TICK_TIME_SECONDS = 60;  // Interval of HBase table flushes in each streamer
	
	private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	
    	this.collector = collector;
    	
        try {
        	// Initialize the streamers with Zookeeper properties.
        	// Specify whether HTable autoflush is enabled (true) or disabled (false).
        	// Specify the number of threads
        	// Specify the length of the LinkedBlockingQueue
            StreamToHBase.init(ZOOKEEPER_QUORUM, ZOOKEEPER_PORT, ZOOKEEPER_NODE, HBASE_WRITE_BUFFER, false, 4, 1000);
        }
        catch (Exception e) {
        	System.out.println(e.getMessage());
        }
    }
 
    /**
     * Writes the HBase message to a streamer.
     * If a tick tuple is found, the streamers will be flushed to HBase.
     * Emits the message tuple once it has been added to the LinkBlockingQueue.
     * 
     * @param tuple the HBase message
     */
    @Override
    public void execute(Tuple tuple) {
    	
    	// Check if tuple is a Tick Tuple
    	if (isTickTuple(tuple)) {
    		StreamToHBase.flushHTable();
		}else{
	        try {
	        	String message = tuple.getString(0);
	        	// Write the message to an HBase streamer
	        	StreamToHBase.writeMessage(message);
	            collector.emit(new Values(message));
	        }
	        catch (Exception e) {
	        	System.out.println(e.getMessage());
	        }
		}
    }

	/**
	 * Specifies the output schema of the tuple as a message.
	 * 
	 * @param declarer is used to declare the output stream ids, outputfields, etc...
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}
	
	/**
	 * Adds the tick tuple frequency to the Storm configuration.
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
	    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICK_TIME_SECONDS);
	    return conf;
	}
	
	/**
	 * Checks if the current tuple is a Tick Tuple.
	 * 
	 * @param tuple the current tuple in the execute() method.
	 */
	public static boolean isTickTuple(Tuple tuple) {
	    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(
	        Constants.SYSTEM_TICK_STREAM_ID);
	  }
 
}
