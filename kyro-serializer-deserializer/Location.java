package StormSerialization;

/**
 * The object definition for a Location
 * 
 * @author Nasheb Ismaily
 *
 */
public class Location {
	
	private String city;
	private String state;
	
	public Location(String city, String state){
		this.city = city;
		this.state = state;
	}
	
	public String getCity(){
		return this.city;
	}
	
	public String getState(){
		return this.state;
	}
	
	public void setCity(String city){
		this.city = city;
	}
	
	public void setState(String state){
		this.state = state;
	}

}
