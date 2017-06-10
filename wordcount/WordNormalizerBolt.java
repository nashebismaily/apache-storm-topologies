package WordCount;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * WordNormalizerBolt will split the sentence into words.
 * 
 * @author Nasheb Ismaily
 */
public class WordNormalizerBolt extends BaseBasicBolt {

	/**
	 * This method tokenizes the sentence and breaks it into individual words.
	 * These words are emitted as a tuple.
	 * 
	 * @param tuple is the input tuple to be processed
	 * @param collector captures and emits the tuples
	 * 
	 */
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// Get the string line from the tuple
		// the '0' indicates the field in the declareOutputFields() method in the Spout
		String sentence = tuple.getString(0);
		// split the sentence into words
		String[] words = sentence.split(" ");
		// Normalize the words
		for(String word : words){
			word = word.trim();
			if(!word.isEmpty()){
				word = word.toLowerCase();
				//Emit the word
				collector.emit(new Values(word));
			}
		}
	}

	/**
	 * This method specifies the output schema of the tuple as a word.
	 * 
	 * @param declarer is used to declare the output stream ids, outputfields, etc...
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}
