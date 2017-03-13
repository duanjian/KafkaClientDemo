import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Created by duan jian on 2017/3/13.
 */
public class JProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.10.139:9092,192.168.10.134:9092,192.168.10.140:9092");
        props.put("acks", "all"); //ack方式，all，会等所有的commit最慢的方式
        props.put("retries", 0); //失败是否重试，设置会有可能产生重复数据
        props.put("batch.size", 16384); //对于每个partition的batch buffer大小
        props.put("linger.ms", 1);  //等多久，如果buffer没满，比如设为1，即消息发送会多1ms的延迟，如果buffer没满
        props.put("buffer.memory", 33554432); //整个producer可以用于buffer的内存大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {

            ProducerRecord<String,String> record = new ProducerRecord<String,String>("my-topic",  Integer.toString(i), Integer.toString(i));
            producer.send(record,
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if(e != null)
                                e.printStackTrace();
                            System.out.println("The offset of the record we just sent is: " + metadata.offset());
                        }
                    });  }

        producer.close();
    }

}
