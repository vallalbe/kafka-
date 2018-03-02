package com.kafka;


import com.weather.RestClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static jodd.util.ThreadUtil.sleep;

public class SimpleProducer {

    public static void main(String[] args) throws Exception {
        // Check arguments length value
        /*if (args.length == 0) {
            System.out.println("Enter topic name");
            return;
        }*/

post();

    }
    public static void post(){
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        System.out.println("Message started....");
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();


        while (true) {
            String result=RestClient.getWeather("11.71,79.52");
            producer.send(new ProducerRecord<String, String>("test", "key", result));
            sleep(10000);
            //producer.close();
        }
    }

}
