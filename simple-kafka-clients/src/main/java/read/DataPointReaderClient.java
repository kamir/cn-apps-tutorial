package read;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class DataPointReaderClient {

    public static String topicName = "t3_MK";

    public static void main(String[] ARGS) {

        Properties props = new Properties();
        // props.load( ... );

        final Consumer<String, GenericRecord> consumer = createConsumer( props );

        consumer.subscribe(Arrays.asList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }

    }


    private static Consumer<String, GenericRecord> createConsumer( Properties props ) {


        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        props.put( "bootstrap.servers", "52.59.203.52:29192");

        props.put( "group.id", "reader1");

        props.put( "schema.registry.url", "http://52.59.203.52:8181");

        return new KafkaConsumer<String, GenericRecord>(props);

    }

}
