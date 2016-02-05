package com;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SimpleJoin {

	public static void main(String[] args) {
		SparkConf sConf = new SparkConf().setAppName("SimpleJoin").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(sConf);
		
		JavaRDD<String> j1 = sc.parallelize(Arrays.asList("a","b","c","a"));
		JavaRDD<String> j2 = sc.parallelize(Arrays.asList("a","b","c","c"));

		JavaPairRDD<String, Integer> p1 = j1.mapToPair(f1 -> new Tuple2<String, Integer>(f1,1)).reduceByKey((a,b) -> a+b);
		JavaPairRDD<String, Integer> p2 = j2.mapToPair(f1 -> new Tuple2<String, Integer>(f1,1)).reduceByKey((a,b) -> a+b);
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> join1 = p1.join(p2);
		
		System.out.println(join1.mapToPair(f -> new Tuple2<String, Integer>(f._1,(f._2._1+f._2._2))).collect());
		
		
		sc.close();
	}

}
