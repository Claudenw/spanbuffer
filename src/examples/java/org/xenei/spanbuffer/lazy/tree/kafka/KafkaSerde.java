package org.xenei.spanbuffer.lazy.tree.kafka;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.tree.TreeLazyLoader;
import org.xenei.spanbuffer.lazy.tree.node.BufferFactory;
import org.xenei.spanbuffer.lazy.tree.serde.SerdeImpl;
import org.xenei.spanbuffer.lazy.tree.serde.TreeDeserializer;
import org.xenei.spanbuffer.lazy.tree.serde.TreeSerializer;

public class KafkaSerde extends SerdeImpl<KafkaPosition> {

    public KafkaSerde(String topic, Properties producerProperties, Properties consumerProperties) {
        super(new KafkaBufferFactory(), new KafkaSerializer(topic, producerProperties),
                new KafkaDeserializer(topic, consumerProperties));
    }

    public static class KafkaBufferFactory implements BufferFactory {

        @Override
        public int bufferSize() {
            return 40 * 1024 * 1024; // 40 MB buffer
        }

        @Override
        public int headerSize() {
            return 0;
        }

        @Override
        public ByteBuffer createBuffer() throws IOException {
            return ByteBuffer.allocate(bufferSize());

        }

        @Override
        public void free(ByteBuffer buffer) throws IOException {
            // does nothing
        }

    }

    public static class KafkaSerializer implements TreeSerializer<KafkaPosition> {

        private KafkaProducer<UUID, ByteBuffer> producer;
        private String topic;

        public KafkaSerializer(String topic, Properties producerProperties) {
            this.topic = topic;
            this.producer = new KafkaProducer<UUID, ByteBuffer>(producerProperties, new UUIDSerializer(),
                    new ByteBufferSerializer());
        }

        @Override
        public int getMaxBufferSize() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int getPositionSize() {
            return KafkaPosition.BYTES;
        }

        @Override
        public KafkaPosition serialize(ByteBuffer buffer) throws IOException {
            long length = buffer.remaining();
            ProducerRecord<UUID, ByteBuffer> record = new ProducerRecord<UUID, ByteBuffer>(topic, UUID.randomUUID(),
                    buffer);
            producer.beginTransaction();
            Future<RecordMetadata> future = producer.send(record);
            producer.commitTransaction();
            try {
                return new KafkaPosition(future.get(), length);
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
        }

        @Override
        public ByteBuffer serialize(KafkaPosition position) throws IOException {
            return position.serialize();
        }

        @Override
        public KafkaPosition getNoDataPosition() {
            return KafkaPosition.NO_POSITION;
        }

    }

    public static class KafkaDeserializer implements TreeDeserializer<KafkaPosition> {
        private String topic;
        private Properties consumerProperties;

        KafkaDeserializer(String topic, Properties consumerProperties) {
            this.topic = topic;
            this.consumerProperties = consumerProperties;
            this.consumerProperties.put("enable.auto.commit", Boolean.FALSE);
        }

        @Override
        public int headerSize() {
            return 0;
        }

        @Override
        public ByteBuffer deserialize(KafkaPosition position) throws IOException {
            if (position.isNoData()) {
                return ByteBuffer.allocate(0);
            }
            try (KafkaConsumer<UUID, ByteBuffer> consumer = new KafkaConsumer<UUID, ByteBuffer>(consumerProperties,
                    new UUIDDeserializer(), new ByteBufferDeserializer())) {
                consumer.seek(position.getPartition(), position.getOffset());
                ConsumerRecords<UUID, ByteBuffer> recs = consumer.poll(Duration.ZERO);
                if (recs.isEmpty()) {
                    throw new IOException("Record not found");
                }
                return recs.iterator().next().value();
            }
        }

        @Override
        public List<TreeLazyLoader<KafkaPosition>> extractLoaders(SpanBuffer buffer) throws IOException {
            List<TreeLazyLoader<KafkaPosition>> result = new ArrayList<TreeLazyLoader<KafkaPosition>>();
            try (DataInputStream dis = new DataInputStream(buffer.getInputStream())) {
                while (true) {
                    try {
                        KafkaPosition position = KafkaPosition.deserialize(topic, dis);
                        TreeLazyLoader<KafkaPosition> tll = new TreeLazyLoader<KafkaPosition>(position.getLength(),
                                position, this);
                        result.add(tll);
                    } catch (EOFException e) {
                        return result;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

}
