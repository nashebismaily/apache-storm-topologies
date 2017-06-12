package HBaseStreaming;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * HBaseStreamSpout will generate a random  id and emit the id as a tuple
 * 
 * @author Nasheb Ismaily
 *
 */
public class HBaseStreamSpout extends BaseRichSpout{
	
	 boolean isDistributed;
	 SpoutOutputCollector collector;

	public HBaseStreamSpout() {
		this(true);
	}

	    public HBaseStreamSpout(boolean isDistributed) {
	        this.isDistributed = isDistributed;
	    }

	    public boolean isDistributed() {
	        return this.isDistributed;
	    }

	    /**
	     * Initialize the output collector
	     * 
	     * @param conf the Storm configuration
	     * @param context the Storm context
	     * @param collector the Storm collector used for emitting messages
	     */
	    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	        this.collector = collector;
	    }

	    /**
	     * Generates the random name from a given array and a random id using Math.random()
	     */
	    public void nextTuple() {
	        final Random rand = new Random();
	        final int id = 1 + (int)(Math.random() * 100000);
	        this.collector.emit(new Values(Integer.toString(id)));
	        Thread.yield();
	    }

		/**
		 * Specifies the output schema of the tuple as an id,name
		 * 
		 * @param declarer is used to declare the output stream ids, outputfields, etc...
		 */
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        declarer.declare(new Fields("id"));
	    }

	}