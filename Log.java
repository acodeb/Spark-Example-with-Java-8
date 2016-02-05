package com;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Log {

	public static void main(String args[]){
		
		SparkConf sConf = new SparkConf().setAppName("log").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sConf);
		
		
		JavaRDD<String> textFile = sc.textFile("src/data/log.txt");
		
		JavaRDD<String> errors = textFile.filter(f -> f.startsWith("ERROR"));
		JavaPairRDD<String, Integer> msg = errors.flatMap(fm -> Arrays.asList(fm.split("\t")))
				.mapToPair(ff -> new Tuple2<String, Integer>(ff,1)).cache();
		
		System.out.println(msg.filter(f1 -> f1._1.contains("mysql")).count());
		System.out.println(msg.filter(f1 -> f1._1.contains("php")).count());
		
		sc.close();
	}
}
