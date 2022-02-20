
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaUseCaseConsumer {
    public static void main(String[] args) {

        //Setup Properties for consumer
        Properties properties = new Properties();

        //List of Kafka brokers to connect to
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "10.0.0.179:49187");

        //Deserializer class to convert Keys from Byte Array to String
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        //Deserializer class to convert Messages from Byte Array to String
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        //Consumer Group ID for this consumer
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,
                "kafka-java-consumer");

        //Set to consume from the earliest message, on start when no offset is
        //available in Kafka
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");

        //Create a Consumer
        KafkaConsumer<String, String> simpleConsumer =
                new KafkaConsumer<String,String>(properties);

        //Subscribe to the kafka.learning.orders topic
        simpleConsumer.subscribe(Arrays.asList("272_topic"));

        //Continuously poll for new messages
        while(true) {

            //Poll with timeout of 5000 milli seconds
            ConsumerRecords<String, String> messages =
                    simpleConsumer.poll(Duration.ofMillis(5000));

            //Print batch of records consumed
            for (ConsumerRecord<String, String> message : messages)
                System.out.println("Message fetched : " + message);
        }



    }
}
