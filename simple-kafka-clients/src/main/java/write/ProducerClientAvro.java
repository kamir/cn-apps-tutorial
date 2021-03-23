package write;

import data.DataPoint;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerClientAvro {

    public static void main(String[] ARGS) throws Exception {

        Properties config = new Properties();

        config.put("client.id", InetAddress.getLocalHost().getHostName());

        config.put( "bootstrap.servers", "52.59.203.52:29192");
        config.put( "acks", "all");
        config.put( "key.serializer", "org.apache.kafka.common.serialization.StringSerializer" );

        config.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        config.put( "schema.registry.url", "http://52.59.203.52:8181");

        /**
         * Data Section for a single message ...
         */
        String topicName = "t3_MK";

        String key = "k3";

        KafkaProducer producer = new KafkaProducer<String, DataPoint>(config);

        int z = 0;
        while (  z < 1000 ) {

            // String value = "SOMETHING - at " + System.currentTimeMillis();
            DataPoint dp = new DataPoint();

            GenericRecord avroRecord = buildRecord( dp );

            final ProducerRecord<String, GenericRecord> record = new ProducerRecord<String, GenericRecord>(topicName, key, avroRecord);

            Future<RecordMetadata> future = producer.send(record);

            System.out.println( z );

            z=z+1;

        }

        producer.flush();
        producer.close();

        /*
        final ProducerRecord<String, String> record = new ProducerRecord<String,String>(topicName, key, value);
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    System.out.println("**** ERROR ****    Send failed for record: " + record);
                    e.printStackTrace();
                }
                else {
                    System.out.println("Send record: " + record);
                }

            }
        });
        */

        System.out.println("Done.");

    }


    public static GenericRecord buildRecord( DataPoint dp ) throws Exception {

        // avro schema avsc file path.
        String schemaPath = "./avro-schema/f1.avsc";

        // avsc json string.
        String schemaString = null;

        FileInputStream inputStream = new FileInputStream(schemaPath);
        try {
            schemaString = IOUtils.toString(inputStream);
        } finally {
            inputStream.close();
        }
        // avro schema.
        Schema schema = new Schema.Parser().parse(schemaString);

        // generic record for data point
        GenericData.Record record = new GenericData.Record(schema);

        // put the elements according to the avro schema.
        record.put( "tsSource", dp.tsSource );
        record.put( "value", dp.value );

        return record;
    }


}

