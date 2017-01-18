package com.sparktest;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

public class StatusTracker {
	
	public static final class IdentityWithDelay<T> implements Function<T,T>
	{

		@Override
		public T call(T arg0) throws Exception {
			Thread.sleep(2000);
			
			return arg0;
		}
		
	}

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\Users\testuser\Downloads\winutils");
		SparkSession ss= SparkSession.builder().master("local").appName("StatusTrackerApp").getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
		JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1,2,3,4,5),5).map(new IdentityWithDelay<Integer>());
		
		JavaFutureAction<List<Integer>> jobfuture = rdd.collectAsync();
		while(!jobfuture.isDone())
		{
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			System.out.println("running");
			
			
		}

	}

}
