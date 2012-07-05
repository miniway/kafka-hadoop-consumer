package kafka.consumer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import kafka.etl.FetchRequest;
import kafka.api.FetchRequest;
import kafka.common.ErrorMapping;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

public class KafkaContext implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(KafkaContext.class);

    SimpleConsumer consumer ;
    String topic;
    int partition;
    long startOffset = -1L;
    long lastOffset = -1L;
    long curOffset;
    int fetchSize;
    ByteBufferMessageSet messages;
    Iterator<MessageAndOffset> iterator;
    
    public KafkaContext(String broker, String topic, int partition, long lastCommit,int fetchSize, int timeout, int bufferSize) {
        String[] sp = broker.split(":");
        consumer = new SimpleConsumer(sp[0], Integer.valueOf(sp[1]), timeout, bufferSize);
        this.topic = topic;
        this.partition = partition;
        this.startOffset = lastCommit;
        this.curOffset = getStartOffset();
        this.lastOffset = getLastOffset();
        this.fetchSize = fetchSize;
        
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }

    private boolean hasMore() {
        if (iterator == null) {
            fetchMore();
             if (iterator == null) {
                 return false;
             }
        }
        boolean hasNext = iterator.hasNext();
        if (hasNext) return hasNext;
        else {
            fetchMore();
            return iterator.hasNext();
        }
    }
    
    private void fetchMore() {
        
        FetchRequest request = 
            new FetchRequest(topic, partition, curOffset, fetchSize);

        messages = consumer.fetch(request);
        int code = messages.getErrorCode();
        if (code == 0) {
            iterator = messages.iterator();
        } else {
            if (code == ErrorMapping.OffsetOutOfRangeCode()){
                LOG.info("OffsetOutOfRange {}-{} Current: {} Last: {}", 
                        new Object[]{topic, partition, curOffset, getLastOffset()});
            } else {
                ErrorMapping.maybeThrowException(code);
            }
        }

    }
    
    public long getNext(LongWritable key, BytesWritable value) throws IOException {
        if ( !hasMore() ) return -1L;
        
        MessageAndOffset messageOffset = iterator.next();
        Message message = messageOffset.message();
        curOffset = messageOffset.offset();

        key.set(curOffset - message.size() - 4);
        byte[] bytes = new byte[message.payloadSize()];
        message.payload().get(bytes);
        value.set(bytes, 0, message.payloadSize());
        
        return curOffset;
    }

    public long getStartOffset() {
        if (startOffset <= 0) {
            startOffset = consumer.getOffsetsBefore(topic, partition, -2L, 1)[0];
        }
        return startOffset;
    }

    public long getLastOffset() {
        if (lastOffset <= 0) {
            lastOffset = consumer.getOffsetsBefore(topic, partition, -1L, 1)[0];
        }
        return lastOffset;
    }

}
