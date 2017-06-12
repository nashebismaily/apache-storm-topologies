package HBaseStreaming;

import java.util.concurrent.LinkedBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 * HBaseStreamers contains the stremers which are represented by the  LinkedBlockingQueues.
 * This class will put and take HBase messages from the queue and add them to the HTable instance.
 * This class also implements the code for HTable flusing. 
 * 
 * @author Nasheb Ismaily
 *
 */
public class HBaseStreamers {
    private Configuration hbaseConfig;
    private Streamer[] streamers;
    private boolean started = false;
 
    private class Streamer implements Runnable {
    	private LinkedBlockingQueue<Put> queue;
        //  private HTable table;
          private String tableName;
          private int counter = 0;
          private int capacity;
          private Connection connection;
          private BufferedMutator mutator;
   
 
        /**
         * Constructs the streamer
         * 
         * @param tableName the HBase table name
         * @param autoFlush whether to flush each message or batch them
         * @param capacity the size of the LinkedBlockingQueue
         */
          public Streamer(String tableName, boolean autoFlush, int capacity) throws Exception {
              // table = new HTable(hbaseConfig, tableName);
              // table.setAutoFlush(autoFlush);
              this.tableName = tableName;
              queue = new LinkedBlockingQueue<Put>(capacity);
              this.capacity = capacity;          
              this.connection = ConnectionFactory.createConnection(hbaseConfig);
              this.mutator = connection.getBufferedMutator(TableName.valueOf(tableName));        
          }
 
        /**
         * Takes data from the LinkedBlockingQueue and adds it to HTable.
         */
        public void run() {
            while (true) {
                try {
                    Put put = queue.take();
                    // table.put(put);
                    this.mutator.mutate(put);
                    counter++;
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
 
        /**
         * Writes the message to the queue.
         * 
         * @param put the HTable put object containing the rowkey, cf, cq, value (message)
         */
        public void write(Put put) throws Exception {
            queue.put(put);
        }
 
        /**
         * Flushes the streamers to HBase
         */
        public void flush() {
            // if (!table.isAutoFlush()) {
                try {
                    // table.flushCommits();
                	this.mutator.flush();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            // }
        }
 
        /**
         * Returns the size of the queue
         */
        public int size() {
            return queue.size();
        }
 
        /**
         * Returns the global counter.
         */
        public int counter() {
            return counter;
        }
    }
 
    /**
     * Creates the streamers, one per thread.
     * 
     * @param quorum the Zookeeper quorum
     * @param port the Zookeeper port
     * @param node the HDP Zookeeper node
     * @param tableName the HBase table name
     * @param autoFlush whether to flush each message to batch
     * @param numOfStreamers the number of streamers/threads
     * @param capacity the size of the LinkedBlockingQueue
     */
    public HBaseStreamers(String quorum, String port, String node, String tableName, String writeBuffer, boolean autoFlush, int numOfStreamers, int capacity) throws Exception {
        hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", quorum);
        hbaseConfig.set("hbase.zookeeper.property.clientPort", port);
        hbaseConfig.set("zookeeper.znode.parent", node);
        hbaseConfig.set("hbase.client.write.buffer", writeBuffer);


        streamers = new Streamer[numOfStreamers];
        for (int i = 0; i < streamers.length; i++) {
            streamers[i] = new Streamer(tableName, autoFlush, capacity);
        }
    }
 
    /**
     * Returns the streamers.
     */
    public Runnable[] getStreamers() {
        return streamers;
    }
 
    /**
     * Starts the threads.
     */
    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        int count = 1;
        for (Streamer streamer : streamers) {
            new Thread(streamer, streamer.tableName + " HBStreamer " + count).start();
            count++;
        }
    }
 
    /**
     * Writes the data to a random streamer
     * 
     * @param put the HBase table object containing the row key, cf, cq, value (message).
     */
    public void write(Put put) throws Exception {
        int i = (int) (System.currentTimeMillis() % streamers.length);
        streamers[i].write(put);
    }
 
    /**
     * Flushes the data in each streamer
     */
    public void flush() {
        for (Streamer streamer : streamers) {
            streamer.flush();
        }
    }
 
    /**
     * Returns the total size of all the streamers
     */
    public int size() {
        int size = 0;
        for (Streamer st : streamers) {
            size += st.size();
        }
        return size;
    }
 
    /**
     * Returns the global counter which tracks the number of puts per streamer
     */
    public int counter() {
        int counter = 0;
        for (Streamer st : streamers) {
            counter += st.counter();
        }
        return counter;
    }
}
