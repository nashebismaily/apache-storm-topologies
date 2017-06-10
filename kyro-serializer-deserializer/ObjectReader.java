package StormSerialization;

import java.util.List;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;


public class ObjectReader extends BaseBasicBolt {

	/**
	 * This method prints the fields in the User object.
	 * The purpose of this bolt is to test the kyro-serializer
	 * 
	 * @param conf provides storm configuration for the bolt
	 * @param context provides complete information about the bolt place within the topology, its task id, input and output information
	 */
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		
		// Get the User object
		User user = (User)tuple.getValueByField("user");
		
		// Get the name,id, locations from the User object
		Integer id = user.getId();
		String name = user.getName();
		List<Location> locations = user.getLocations();
	
		// Print the User object fields
		System.out.println("id = " + id + ", name =" + name);
		for(Location location : locations){
			System.out.println("city = " + location.getCity() + ", state = " + location.getState());
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
