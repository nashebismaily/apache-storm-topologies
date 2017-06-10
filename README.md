# Storm Topologies

This is a collection of Apache Storm topologies using the new Storm 1.x APIs

## Word Count

This topology will read a text file and count the number of times each word appears.  
The output is printed to the console once the Spout has shut down.  
The topology will run in Local Mode.  

## Kyro Serializer Deserializer

This topology will create a User object that contains a Location object.  
These objects are serialized/deserialized in Storm using a custom Kyro Serializer/Deserializer.  
The topology will run in Local Mode and tests the kyro serialization framework.  

## Author

Nasheb Ismaily
