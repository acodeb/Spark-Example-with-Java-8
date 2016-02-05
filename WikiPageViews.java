package com;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WikiPageViews {

	public static void main(String[] args) {

		SparkConf sConf = new SparkConf().setAppName("WikiPageView").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(sConf);
		
		JavaRDD<String> input = sc.textFile("src/data/wikidata/pageviews_by_second.tsv");
		
		
		JavaRDD<List<String>> data = input.map(line -> Arrays.asList(line.replaceAll("\"", "").split("\t")))
														     .filter(f -> !f.get(2).contains("requests")).cache();
		//Calculate sum of mobile/Desktop request separately.
		JavaPairRDD<String, Integer> sumBySite = data.mapToPair(m1 -> new Tuple2<String, Integer>(m1.get(1), Integer.parseInt(m1.get(2))))
											  		 .reduceByKey((a,b) -> a+b);

		System.out.println(sumBySite.collect());
		
		//Calculate sum of requests by timestamp.
		JavaPairRDD<LocalDateTime, Integer> sumByTimeStamp = data.mapToPair(mp -> new Tuple2<LocalDateTime, Integer>(LocalDateTime.parse(mp.get(0)), Integer.parseInt(mp.get(2))))
										 			  	.reduceByKey((a,b) -> a+b)
										 			  	.sortByKey();
		
		
		for(Tuple2<LocalDateTime, Integer> t:sumByTimeStamp.take(10)){
			System.out.println(t);
		}
		
		sc.close();
	}

}
