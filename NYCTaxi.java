package com;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class NYCTaxi {

	public static void main(String[] args) {
		SparkConf sConf = new SparkConf().setAppName("NYCTaxi").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(sConf);
		
		JavaRDD<String> file = sc.textFile("/Users/ankitjindal/Documents/Apache_Spark/Tutorials_and_Certification/Spark_offline_current/labfiles/nyctaxi/nyctaxi.csv");
		
		JavaRDD<List<String>> split = file.map(m1 -> Arrays.asList(m1.split(",")));
		JavaPairRDD<String, Integer> a = split.mapToPair(m -> new Tuple2<String, Integer>(m.get(6),1))
											  .reduceByKey((a1,b1) -> a1+b1);
		
		
		/*for (Tuple2<String, Integer> t : a.top(10, new CustomComaprator())){
			System.out.println(t);
		}*/
		a.top(10, new CustomComaprator()).forEach(f -> System.out.println(f));
		sc.close();
	}

}

class CustomComaprator implements Serializable, Comparator<Tuple2<String, Integer>>
{ 

	private static final long serialVersionUID = 1L;

	public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
		
		return o1._2.compareTo(o2._2);
	} 
}