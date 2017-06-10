package WordCount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

/**
 * WordReaderSpout will read a file and emit each line as a sentence.
 * 
 * @author Nasheb Ismaily
 */
public class WordReaderSpout extends BaseRichSpout {

	// Create instance for SpoutOutputCollector which passes tuples to the bolt
	private SpoutOutputCollector collector;
	private boolean completed = false;
	
	// Create instance for TopologyContext which contains topology data
	private TopologyContext context;
	
	// Create a FileReader handle
	FileReader fileReader;
	
	/**
	 * This method will create a new file reader instance for the text file.
	 * 
	 * @param conf provides storm configuration for the spout
	 * @param context provides complete information about the spout place within the topology, its task id, input and output information
	 * @param collector enables us to emit the tuple that will be processed by the bolts
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// Create a new File Reader with the 'wordFile' object passed in the configuration
		String file = conf.get("wordsFile").toString();
		try {
			this.fileReader = new FileReader(file);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file: " + file);
		}
		this.context = context;
		this.collector = collector;
	}

	/**
	 * nextTuple() is called periodically from the same loop as the ack() and fail() methods. 
	 * It must release control of the thread when there is no work to do, so that other methods have a chance to be called.
	 * So the first line of nextTuple() checks to see if processing has finished.
	 * If so, it should sleep for at least one millisecond to reduce load on the processor before returning.
	 */
	@Override
	public void nextTuple() {  

		// Check if completed, if so then thread will sleep
		if(completed){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {	
			  // Do Nothing
		}
			return;
		}
		
		//  Read Each line from the file
		BufferedReader reader = new BufferedReader(this.fileReader);
		String line;
		try {
			while((line = reader.readLine()) != null){
			// emit the line as well as the message ID
			String messageId = line;
			this.collector.emit(new Values(line), messageId);
			}
		} catch (Exception e) { // If there is an error reading the tuple then throw a runtime exception
			throw new RuntimeException("Error reading tuple",e);
		} finally{  // set the thread to completed state
			this.completed = true;
		}
	}
  
	/**
	 * This method specifies the output schema of the tuple as a sentence.
	 * 
	 * @param declarer is used to declare the output stream ids, outputfields, etc...
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
}