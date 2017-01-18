package com.sparktest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;



import org.apache.kafka.clients.consumer.ConsumerRecord;

import scala.Tuple2;
public class SparkKafkaConsumer {

	public static void main(String args[])
	{
		
		SparkConf sc= new SparkConf().setAppName("kafka-streaming-word-count").setMaster("local[2]");
		System.setProperty("hadoop.home.dir", "C:\\Users\\testuser\\Downloads\\winutils");
		
		
		JavaStreamingContext jsc = new JavaStreamingContext(sc,new Duration(2000));
//		Map<String,Integer> topicsMap = new HashMap<String,Integer>();
//		topicsMap.put("test", 1);
//		
//		
//		
//		JavaPairReceiverInputDStream<String,String> messages = (JavaPairReceiverInputDStream<String, String>) KafkaUtils.createStream(jsc, "0.0.0.0:2181","my-consumer-group" , topicsMap);
//		 JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
//		      @Override
//		      public String call(Tuple2<String, String> tuple2) {
//		        return tuple2._2();
//		      }
//		    });
//		
//		lines.print();
//		jsc.start();
//		try {
//			jsc.awaitTermination();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
		
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "default");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		java.util.Collection<String> topics = (java.util.Collection<String>) Arrays.asList("test");

		final JavaInputDStream<ConsumerRecord<String, String>> stream =
		  (org.apache.spark.streaming.api.java.JavaInputDStream) KafkaUtils.createDirectStream(
		    jsc,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
		  );

		org.apache.spark.streaming.api.java.JavaPairDStream<String,String> rdd= (org.apache.spark.streaming.api.java.JavaPairDStream) stream.mapToPair(
		  new PairFunction<ConsumerRecord<String, String>, String, String>() {
		    @Override
		    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
		      return new Tuple2<>(record.key(), record.value());
		    }
		  });
		
//	JavaDStream words =	rdd.flatMap(new FlatMapFunction<String,String> ());
		
		
		
		
		
		
		rdd.print();
		 jsc.start();
		    try {
				jsc.awaitTermination();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		//stream.print();
	}
	
}
