package StormSerialization;

import java.util.ArrayList;
import java.util.List;

/**
 * The object definition for a User
 * 
 * @author Nasheb Ismaily
 *
 */
public class User {
	
	private Integer id;
	private String name;
	private List<Location> locations = new ArrayList<Location>();


	public Integer getId(){
		return this.id;
	}
	
	public String getName(){
		return this.name;
	}
	
	public List<Location> getLocations(){
		return this.locations;
	}
	
	public void setId(Integer id){
		this.id = id;
	}
	
	public void setName(String name){
		this.name = name;
	}
	
	public void setLocations(List<Location> locations){
		this.locations = locations;
	}
	
	public void addLocation(Location location){
		this.locations.add(location);
	}
	
}
