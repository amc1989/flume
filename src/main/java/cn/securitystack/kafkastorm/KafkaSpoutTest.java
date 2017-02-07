package cn.securitystack.kafkastorm;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class KafkaSpoutTest implements IRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private KafkaConsumer<String, String> consumer;
	private String topic = "idoall_testTopic";

	public KafkaConsumer<String, String> getConsumer() {
		Properties props = new Properties();
		/* 定义kakfa 服务的地址，不需要将所有broker指定上 */
		props.put("bootstrap.servers", "192.168.159.10:9092");
		/* 制定consumer group */
		props.put("group.id", "1");
		/* 是否自动确认offset */
		props.put("enable.auto.commit", "true");
		/* 自动确认offset的时间间隔 */
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		/* key的序列化类 */
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		/* value的序列化类 */
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		/* 定义consumer */
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		/* 消费者订阅的topic, 可同时订阅多个 */
		consumer.subscribe(Arrays.asList(topic));
		return consumer;

	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}
    
	@Override
	public void activate() {
		consumer = getConsumer();
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
		System.out.println("consumerRecord   size :" + records.toString());
		for (ConsumerRecord<String, String> record : records) {
			String value = record.value();
			System.out.println("storm接收到来自kafka的消息------->" + value);
			SimpleDateFormat formatter = new SimpleDateFormat(
					"yyyy年MM月dd日 HH:mm:ss");
			Date curDate = new Date(System.currentTimeMillis());// 获取当前时间
			String str = formatter.format(curDate);
			collector.emit(new Values(record.value(), 1, str), value);
		}

	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","id","time"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		System.out.println("getComponentConfiguration被调用");
		topic = "idoall_testTopic";
		return null;
	}

}
