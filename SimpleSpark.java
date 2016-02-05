package com;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SimpleSpark {

	public static void main(String[] args) {
		SparkConf sconf = new SparkConf().setAppName("SimpleSpark").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sconf);
		
		JavaRDD<String> data = sc.parallelize(Arrays.asList("a","b","c","a"));
		
		JavaPairRDD<String, Integer> a = data.mapToPair(f -> new Tuple2<String, Integer>(f,1)).reduceByKey((a1,b1) -> a1+b1);
		
		System.out.println(a.collect());
		
		
		JavaRDD<Double> d = sc.parallelize(Arrays.asList(1.0,2.0,3.0));
		
		JavaDoubleRDD dd = d.mapToDouble(f -> f*f);
		
		System.out.println(dd.variance());
		
		sc.close();
	}

}
