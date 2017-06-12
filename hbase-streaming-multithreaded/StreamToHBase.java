package HBaseStreaming;

import java.util.UUID;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * StreamToHBase is a wrapper around HBaseStreamers.
 * This method is accessed by the HBaseStreamBolt inorder to write and flush tuples into HBase. 
 * 
 * @author Nasheb Ismaily
 *
 */
public class StreamToHBase {
    private static final String tableName = "stormhbtest";
    private static final byte[] colFamily = Bytes.toBytes("data");
    private static final byte[] colQualifier = Bytes.toBytes("message");
    private static boolean isInit = false;
    private static HBaseStreamers hbStreamers = null;
    
    /**
     * Initializes the HBase streamers that will write the data to HBase.
     * 
     * @param zkQuorum the HDP Zookeeper Quorum
     * @param zkPort the HDP Zookeeper Port
     * @param zkNode the HDP Zookeeper Node
     * @param autoFlush	whether or not to flush each put into HBase 
     * @param numOfStreamers the number of streamers to create
     * @param queueCapacity the size of the LinkedBlockingQueue
     */
    public static synchronized void init(String zkQuorum, String zkPort, String zkNode, String writeBuffer, boolean autoFlush, int numOfStreamers, int queueCapacity)
        throws Exception {
        if (isInit == true)
            return;
        isInit = true;
        HBaseStreamers streamers = new HBaseStreamers(zkQuorum, zkPort, zkNode, tableName, writeBuffer, autoFlush, numOfStreamers, queueCapacity);
        streamers.start();
        hbStreamers = streamers;
  
    }
 
    /**
     * Adds the write to the streamer (LinkedBlockingQueue).
     * 
     * @param message the data that will be added to the HBase value cell.
     */
    public static void writeMessage(String message) throws Exception {
        byte[] value = Bytes.toBytes(message);
        byte[] rowIdBytes = Bytes.toBytes(UUID.randomUUID().toString());
        Put p = new Put(rowIdBytes);
        p.add(colFamily, colQualifier, value);
        if (hbStreamers != null) {
            hbStreamers.write(p);
        }
    }
    
    /**
     * Flushes the HBase table puts in each streamer.
     */
    public static void flushHTable() {
    	hbStreamers.flush();        
    }
}
