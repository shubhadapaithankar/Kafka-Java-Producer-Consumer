

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaUseCaseProducer {

    public static void main(String[] args) {

        //Setup Properties for Kafka Producer
        Properties properties = new Properties();

        //List of brokers to connect to
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "10.0.0.179:49187");

        //Serializer class used to convert Keys to Byte Arrays
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        //Serializer class used to convert Messages to Byte Arrays
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        //Create a Kafka producer from configuration
        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        //Publish 10 messages at 2 second intervals, with a random key
        try{

            for( int i=0; i < 10; i++) {
                //Create a producer Record
                ProducerRecord<String,String> producerRecord =
                        new ProducerRecord<String,String>(
                                "272_topic",    //Topic name
                                String.valueOf(i),          //Key for the message
                                "This is project group 24 : i =" + i         //Message Content
                        );

                System.out.println("Sending Message : "+ producerRecord);

                //Publish to Kafka
                kafkaProducer.send(producerRecord);

                Thread.sleep(5000);
            }
        }
        catch(Exception e) {

        }
        finally {
            kafkaProducer.close();
        }

    }
}
