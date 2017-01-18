package com.sparktest;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;



import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple12;
import scala.Tuple2;


public class SparkWordCount {

	
	
	public static void main(String args[])
	{
		
		System.setProperty("hadoop.home.dir", "C:\\Users\\testuser\\Downloads\\winutils");
		SparkConf conf= new SparkConf().setAppName("SparkTest").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
	// reads the input file
		JavaRDD<String> file = jsc.textFile("data\\sampleinput.txt");
	// split the input interms of words
		
		System.out.println("sample spark job invoked");
		JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String arg0) throws Exception {
				 java.util.List listItem=  (java.util.List) Arrays.asList(arg0.split(" "));
			     return listItem.iterator();
			}
		});
//		JavaRDD<String> words=  file.flatMap(new FlatMapFunction<String,String>() { 
//			
//
//			public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
//
//			
//			
//		
//		});
		
	// iterate words and add a count as 1 to each word. later reduce will group them
		JavaPairRDD<String,Integer> pairs= words.mapToPair(new PairFunction<String,String,Integer>(){

			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
			//System.out.println("in map to pair"+s);
			return new Tuple2<String,Integer>(s,1);
			}
			
			
		});
		
		
		JavaPairRDD<String,Integer> count = pairs.reduceByKey(new Function2<Integer,Integer,Integer>(){

			@Override
			public Integer call(Integer a, Integer b) throws Exception {
			
				return a+b;
			}
			
		});
		
		
	     Iterator iterate=count.collect().iterator();
	     while(iterate.hasNext())
	     {
	    System.out.println(iterate.next().toString());
	     }
		
		
//		JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
//			  public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
//			});
		
		
	}
}
 