package SparkKafkaElastic.SparkKafkaElastic;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.spark_project.guava.collect.ImmutableMap;

public class KafkaConsumerElastic {
	public static final String topicKafka = "topicsayafan";
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Consumer to Elastic")
				.set("es.index.auto.create", "true")
				.set("spark.ui.port", "9099")
				.set("spark.driver.allowMulipleContexts", "true")
				.set("es.nodes", "localhost:9200");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.OFF);
		
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "coba");
		
		Collection<String> kafkaTopic = Arrays.asList(topicKafka.split(","));
		

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
				.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(kafkaTopic, props));
		
		JavaDStream<String> recordData = stream.map(new Function<ConsumerRecord<String,String>, String>() {
			
			@Override
			public String call(ConsumerRecord<String, String> datas) throws Exception {
				// TODO Auto-generated method stub
				return datas.value();
			}
		});
		
		try {
			recordData.foreachRDD(e -> {
//				System.out.println("saving.....");
//				System.out.println(e);
				JavaEsSpark.saveJsonToEs(e, "spark-daerah-aja/_doc",ImmutableMap.of("es.mapping","id"));
				System.out.println("sukses");
				System.exit(0);
//				JavaEsSpark.saveJsonToEs(e, "spark-hehe/_doc", ImmutableMap.of("es.mapping", "id"));
			});
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
//		System.out.println(e.getMessage());
		}
		jssc.start();
		jssc.awaitTermination();
	}
}
