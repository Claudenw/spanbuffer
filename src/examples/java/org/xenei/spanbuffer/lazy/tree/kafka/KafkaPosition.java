package org.xenei.spanbuffer.lazy.tree.kafka;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.xenei.spanbuffer.lazy.tree.serde.Position;

public class KafkaPosition implements Position {
    private long offset;
    private int partition;
    private long length;
    private String topic;

    public static KafkaPosition NO_POSITION = new KafkaPosition("", 0, 0, 0);
    public static int BYTES = (2 * Long.BYTES) + Integer.BYTES;

    private KafkaPosition(String topic, int partition, long offset, long length) {
        if (partition < 0) {
            throw new IllegalArgumentException("Partition may not be less than zero (0)");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset may not be less than zero (0)");
        }
        if (length < 0) {
            throw new IllegalArgumentException("Length may not be less than zero (0)");
        }
        this.topic = topic.trim();
        this.offset = offset;
        this.partition = partition;
        this.length = length;
    }

    KafkaPosition(RecordMetadata metadata, long length) {
        if (length < 0) {
            throw new IllegalArgumentException("Length may not be less than zero (0)");
        }

        this.topic = metadata.topic();
        this.offset = metadata.offset();
        this.partition = metadata.partition();
        this.length = length;
    }

    public ByteBuffer serialize() {
        ByteBuffer buff = ByteBuffer.allocate(BYTES);
        buff.asIntBuffer().put(partition);
        buff.asLongBuffer().put(offset).put(length);
        buff.flip();
        return buff;
    }

    public static KafkaPosition deserialize(String topic, DataInputStream inputStream) throws IOException {
        int partition = inputStream.readInt();
        long offset = inputStream.readLong();
        long length = inputStream.readLong();
        return new KafkaPosition(topic, partition, offset, length);

    }

    public TopicPartition getPartition() {
        return new TopicPartition(topic, partition);
    }

    @Override
    public boolean isNoData() {
        return length <= 0;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

}