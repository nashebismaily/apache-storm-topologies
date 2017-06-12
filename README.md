# Storm Topologies

This is a collection of advanced Apache Storm topologies using the new Storm 1.x APIs

## Kyro Serializer Deserializer

This topology will create embedded objects that are serialized/deserialized in Storm using a custom Kyro Serializer/Deserializer.  
The topology will run in Local Mode and tests the kyro serialization framework.

## HBase Streaming Multithreaded

This topology will creates a LinkedBlockingQueues and multiple Streamers (each represented by a thread).  
The Hbase bolt will add table puts to the LinkedBlockingQueue.  
Each streamer (thread) will then pull HBase table puts from the queue, batch them, and write them to HBase.  
Tick tuples have been added to aid in table flushing.  
HBase client side write buffer is presented as an end user defined configuration.  

## Author

Nasheb Ismaily
