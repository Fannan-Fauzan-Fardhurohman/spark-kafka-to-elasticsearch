package SparkKafkaElastic.SparkKafkaElastic;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaProducerJava {
	public static int num = 1;
	public static void main(String[] args) throws Exception {
		final ObjectMapper om = new ObjectMapper();
		
		
		SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("Producer Kafka");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");
		
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		
		JavaRDD<String> jsonFile = sc.textFile("/home/ebdesk/Documents/daerah.json").flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String datas) throws Exception {
				// TODO Auto-generated method stub
				JsonNode node = om.readTree(datas);
				List<String> result = new ArrayList<String>();
				for(JsonNode n:node) {
					result.add(om.writeValueAsString(n));
				}
				return result.iterator();
			}
		});
		
		try {
			List<String> datas = jsonFile.collect();
			for(String d:datas) {
				Thread.sleep(1000);
				ProducerRecord<String, String> record = new ProducerRecord<String, String>("topicsayafan", d);
				producer.send(record);
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

}