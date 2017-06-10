package StormSerialization;

import java.util.Random;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * ObjectCreatorBolt will create a new user object with a name, id, city, and state.
 * The User object is serizlied using the custom Kyro Serializer/Deserializer.
 * 
 * @author Nasheb Ismaily
 *
 */
public class ObjectCreatorBolt extends BaseBasicBolt {
	
	/**
	 * This method creates the user object using the name and id emitted from the spout.
	 * A random city is generated for the user object from a predefined list.
	 * 
	 * @param conf provides storm configuration for the bolt
	 * @param context provides complete information about the bolt place within the topology, its task id, input and output information
	 */
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		// Get the emitted values from the Spout
		Integer id = tuple.getIntegerByField("id");
		String name = tuple.getStringByField("name");
		
		//Create the User object and set the name, id
		User user = new User();
		user.setId(id);
		user.setName(name);
		
        Random rand = new Random();
	    String[] cities = new String[] { "houston", "austin", "dallas", "san antonio", "el paso" };
	    String state = "texas";
		int counter = 1 + (int)(Math.random() * 10);
		for(int i = 0; i< counter; i++){
	        String city = cities[rand.nextInt(cities.length)];
	        Location location = new Location(city,state);
	        //Set the city and state for the user
	        user.addLocation(location);
		}
		
		collector.emit(new Values(user));
	}

	/**
	 * This method specifies the output schema of the tuple as a user.
	 * 
	 * @param declarer is used to declare the output stream ids, outputfields, etc...
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user"));
	}

}
