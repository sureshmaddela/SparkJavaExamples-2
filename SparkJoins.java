package com.sparktest;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkJoins {

	public static void main(String[] args) {
		
		
		SparkConf sc= new SparkConf().setAppName("SparkJoins").setMaster("local");
		JavaSparkContext  jsc = new JavaSparkContext(sc); 
        JavaRDD<String> customersData=jsc.textFile("data\\customer_data.txt");
        JavaPairRDD<String,String> customerPair = customersData.mapToPair( new PairFunction(){

			@Override
			public Tuple2<String,String> call(Object arg0) throws Exception {
				String[] customerList =((String) arg0).split(",");
				return new Tuple2<String,String>(customerList[0],customerList[1]);
			}
        	
        }); 
        
        Iterator iterate=customerPair.collect().iterator();
	     while(iterate.hasNext())
	     {
	    System.out.println(iterate.next().toString());
	     }
	     
	     
	     JavaRDD<String> transactionData = jsc.textFile("data\\transaction_data.txt");
	     JavaPairRDD<String,String> transactionPair = transactionData.mapToPair(new PairFunction(){

			@Override
			public Tuple2 call(Object arg0) throws Exception {
				String[] transactionList = ((String) arg0).split(",");
				return new Tuple2<String,String>(transactionList[2],transactionList[4]);
			}
	    	 
	     });
	     
	     
	     Iterator transactIterator = transactionPair.collect().iterator();
	     while(transactIterator.hasNext())
	     {
	    	 System.out.println(transactIterator.next());
	     }
	     
	     
	     JavaPairRDD defaultJoin = customerPair.join(transactionPair);
	     System.out.println(defaultJoin.collect());
	     
	     
	     JavaPairRDD fullOuterJoin = customerPair.fullOuterJoin(transactionPair);
	     System.out.println(fullOuterJoin.collect());
	     
	     
	     
	}

}
