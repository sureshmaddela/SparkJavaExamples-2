package com.sparktest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class SparkSQL {

	public static void main(String[] args) {
		
		
		SparkSession session = SparkSession.builder().master("local").appName("SparkSQLTEST").getOrCreate();
		

		Dataset<Row> df = session.read().json("data\\people.json");
		//df.schema();
		df.show();
		df.select("name").show();
		df.select(col("name"), col("age")).show();
		df.filter(col("age").gt(21)).show();
		df.groupBy("age").count().show();
		df.createOrReplaceTempView("employees");
		
		Dataset<Row>  sqlDS= session.sql("select * from employees");
		System.out.println("after view");
		sqlDS.show();
		session.stop();
	}

}
 