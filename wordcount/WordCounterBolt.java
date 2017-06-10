package WordCount;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * WordCounterBolt will count the number of times each word appears in the text.
 * @author Nasheb Ismaily
 */
public class WordCounterBolt extends BaseBasicBolt {

	//Task ID
	private Integer id;
	//Component ID
	private String name;
	//Map which contains the words and their counts
	private Map<String, Integer> counters;
	
	/**
	 * This method initializes the private variables of this class.
	 * 
	 * @param conf provides storm configuration for the bolt
	 * @param context provides complete information about the bolt place within the topology, its task id, input and output information
	 */
	@Override
	public void prepare(Map conf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	/**
	 * This method counts the instances of each word and stores the results in a HashMap.
	 * 
	 * @param tuple is the input tuple to be processed
	 * @param collector captures and emits the tuples
	 * 
	 */
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String str = tuple.getString(0);
		/*
		* If the word dosn't exist in the map we will create
		* this, if not We will add 1
		*/
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		}else{
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
	}

	/**
	* This method is run when the spout has shutdown and there are no more words to process
	*/
	@Override
	public void cleanup() {
		// Print the Values in the Counter Map
		System.out.println("-- Word Counter ["+name+"-"+id+"] --");
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
		}
	}

	/**
	 * This method does nothing since we are not emitting a tuple
	 * 
	 * @param declarer is used to declare the output stream ids, outputfields, etc...
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Not emitting anything 
	}

}
