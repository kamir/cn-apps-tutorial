package write;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class ProducerClientJson {

    public static void main(String[] ARGS) throws UnknownHostException {

        Properties config = new Properties();

        config.put("client.id", InetAddress.getLocalHost().getHostName());

        config.put( "bootstrap.servers", "52.59.203.52:29192");
        config.put( "acks", "all");
        config.put( "key.serializer", "org.apache.kafka.common.serialization.StringSerializer" );
        config.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");


        config.put( "schema.registry.url", "http://52.59.203.52:8181");


        /**
         * Data Section for a single message ...
         */
        String topicName = "t2_MK";

        String key = "k2";

        KafkaProducer producer = new KafkaProducer<String, DataPointJSON>(config);

        int z = 0;
        while (  z < 10 ) {

            // String value = "SOMETHING - at " + System.currentTimeMillis();
            DataPointJSON dp = new DataPointJSON();

            final ProducerRecord<String, DataPointJSON> record = new ProducerRecord<String, DataPointJSON>(topicName, key, dp);

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

}

class DataPointJSON {

    public Long tsSource = null;
    public Double value = null;

    Random rand = new Random();

    public DataPointJSON() {
        tsSource = System.currentTimeMillis();
        value = rand.nextDouble();
    }

}
