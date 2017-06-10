package StormSerialization;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * WordSpout will generate a random name and a random id and emit the name,id tuple
 * 
 * @author Nasheb Ismaily
 *
 */
public class WordSpout extends BaseRichSpout{
	
	 boolean isDistributed;
	    SpoutOutputCollector collector;
	    public static final String[] names = new String[] { "bob", "mike", "sam", "lary", "brian" };

	    public WordSpout() {
	        this(true);
	    }

	    public WordSpout(boolean isDistributed) {
	        this.isDistributed = isDistributed;
	    }

	    public boolean isDistributed() {
	        return this.isDistributed;
	    }

	    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	        this.collector = collector;
	    }

	    /**
	     * This method will generate the random name from a given array and a random id using Math.random()
	     */
	    public void nextTuple() {
	        final Random rand = new Random();
	        final String name = names[rand.nextInt(names.length)];
	        final int id = 1 + (int)(Math.random() * 100000);
	        this.collector.emit(new Values(id,name), UUID.randomUUID());
	        Thread.yield();
	    }

		/**
		 * This method specifies the output schema of the tuple as an id,name
		 * 
		 * @param declarer is used to declare the output stream ids, outputfields, etc...
		 */
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        declarer.declare(new Fields("id", "name"));
	    }

	}