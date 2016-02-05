package com;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountWithJoin {

	public static void main(String[] args) {

		SparkConf sConf = new SparkConf().setAppName("WordCountWithJoin").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sConf);
		
		JavaRDD<String> readme = sc.textFile("src/data/README.md");
		JavaRDD<String> contrib = sc.textFile("src/data/CONTRIBUTING.md");
		
		JavaPairRDD<String, Integer> r = readme.flatMap(fm -> Arrays.asList(fm.split(" ")))
											   .filter(f -> f.equals("Spark"))
											   .mapToPair(mp -> new Tuple2<String, Integer>(mp, 1))
											   .reduceByKey((r1,r2) -> r1+r2);
		
		JavaPairRDD<String, Integer> c = contrib.flatMap(fm -> Arrays.asList(fm.split(" ")))
												.filter(f -> f.equals("Spark"))
												.mapToPair(mp -> new Tuple2<String, Integer>(mp,1))
												.reduceByKey((r1,r2) -> r1+r2);
		
		System.out.println(r.join(c).collect());
		
		sc.close();
	}

}
