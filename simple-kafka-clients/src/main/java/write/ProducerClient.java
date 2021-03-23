package write;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerClient {

    public static void main(String[] ARGS) throws UnknownHostException {

        Properties config = new Properties();

        config.put("client.id", InetAddress.getLocalHost().getHostName());

        config.put( "bootstrap.servers", "52.59.203.52:29192");
        config.put( "acks", "all");
        config.put( "key.serializer", "org.apache.kafka.common.serialization.StringSerializer" );
        config.put( "value.serializer", "org.apache.kafka.common.serialization.StringSerializer" );

        /**
         * Data Section for a single message ...
         */
        String topicName = "t1_MK";
        String key = "k1";

        KafkaProducer producer = new KafkaProducer<String, String>(config);

        int z = 0;
        while (  z < 100 ) {

            String value = "SOMETHING - at " + System.currentTimeMillis();

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);
            Future<RecordMetadata> future = producer.send(record);

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
