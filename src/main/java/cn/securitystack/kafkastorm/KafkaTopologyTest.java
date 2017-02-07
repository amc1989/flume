package cn.securitystack.kafkastorm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class KafkaTopologyTest {

	public static void main(String[] aeges) {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new KafkaSpoutTest(), 1);
		builder.setBolt("bolt1", new Bolt1(), 2).shuffleGrouping("spout");
		builder.setBolt("bolt2", new Bolt2(), 2).fieldsGrouping("bolt1",
				new Fields("word"));

		Map conf = new HashMap();
		conf.put(Config.TOPOLOGY_WORKERS, 1);
		conf.put(Config.TOPOLOGY_DEBUG, true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("my-flume-kafka-storm-topology-integration",
				conf, builder.createTopology());

		Utils.sleep(1000 * 60 * 5); // local cluster test ...
		cluster.shutdown();
	}

	public static class Bolt1 extends BaseBasicBolt {

		public void execute(Tuple input, BasicOutputCollector collector) {
			try {
				String msg = input.getString(0);
				int id = input.getInteger(1);
				String time = input.getString(2);
				msg = msg + "bolt1";
				System.out.println("对消息加工第1次-------[arg0]:" + msg
						+ "---[arg1]:" + id + "---[arg2]:" + time + "------->"
						+ msg);
				if (msg != null) {
					collector.emit(new Values(msg));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));

		}
	}

	public static class Bolt2 extends BaseBasicBolt {
		Map<String, Integer> counts = new HashMap<String, Integer>();

		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String msg = tuple.getString(0);
			msg = msg + "bolt2";
			System.out.println("对消息加工第2次---------->" + msg);
			collector.emit(new Values(msg, 1));
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
	}
}
