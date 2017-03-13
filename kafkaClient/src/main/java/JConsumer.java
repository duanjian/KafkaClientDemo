import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by duan jian on 2017/3/13.
 */
public class JConsumer {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.10.139:9092,192.168.10.134:9092,192.168.10.140:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");  //自动commit
        props.put("auto.commit.interval.ms", "1000"); //定时commit的周期
        props.put("session.timeout.ms", "30000"); //consumer活性超时时间
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);
        consumer.subscribe(Arrays.asList("my-topic")); //subscribe，foo，bar，两个topic
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);  //100是超时等待时间
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
        }
    }
}
