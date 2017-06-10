package StormSerialization;

import java.io.Serializable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * This kyro-serializer defines how Storm will serialize/deserialize the User object
 * 
 * @author Nasheb Ismaily
 *
 */
public class TupleKyroSerializer extends com.esotericsoftware.kryo.Serializer<User> implements
Serializable {

	// Serialize
	public void write( Kryo kryo, Output output, User user ) {
		// Write down all the fields of the object
		output.writeInt(user.getId());
		output.writeString( user.getName());
	}

	// Deserialize
	public User read( Kryo kryo, Input input, Class<User> aClass ) {
		// Read all the fields of the object
		User user = new User( );
		user.setId(input.readInt());	
		return user;
	}
}
