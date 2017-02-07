package cn.securitystack.kafkasink;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSink extends AbstractSink implements Configurable {
	// kafka生产者
	private KafkaProducer<String, String> producer;
	private  String topic ;
	private Log log = LogFactory.getLog(this.getClass());

	public Status process() throws EventDeliveryException {
		//获取通道
		Channel channel = getChannel();
		//获取通道事务，通过事务保证数据的ACID特性
        Transaction tx = channel.getTransaction();
        try {
       //开始事务
            tx.begin();
       //flume中的数据的最小的基本单位就是event
            Event e = channel.take();
            if (e == null) {
                tx.rollback();
                return Status.BACKOFF;
            }
            //封装flume的数据到kafka中
            ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, new String(e.getBody()));
            producer.send(data);
            log.info("flume向kafka发送消息：" + new String(e.getBody()));
            tx.commit();
            return Status.READY;
        } catch (Exception e) {
            log.error("Flume KafkaSinkException:", e);
            tx.rollback();
            return Status.BACKOFF;
        } finally {
            tx.close();
        }
    }
	

	public void configure(Context context) {
		topic = "idoall_testTopic";
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.159.10:9092");
		props.put("acks", "1");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("num.partitions", "3"); //
		props.put("request.required.acks", "1");
		producer = new KafkaProducer<String,String>(props);
		log.info("KafkaSink初始化完成.");

	}

}
