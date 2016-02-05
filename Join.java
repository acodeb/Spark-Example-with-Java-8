package com;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Join{

	public static void main(String args[]){
		SparkConf sConf = new SparkConf().setAppName("join").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(sConf);
		
		JavaRDD<String> reg = sc.textFile("src/data/reg.tsv");
		JavaRDD<String> clk = sc.textFile("src/data/clk.tsv");
		
		JavaPairRDD<String, Register> r1 = reg.map(f1 -> Arrays.asList(f1.split("\t")))
												.mapToPair(f2 -> new Tuple2<String, Register>(f2.get(1),new Register(f2.get(0), 
																													 f2.get(1), 
																													 Integer.parseInt(f2.get(2)),
																													 Float.parseFloat(f2.get(3)),
																													 Float.parseFloat(f2.get(4)))));
		JavaPairRDD<String, Click> r2 = clk.map(f3 -> Arrays.asList(f3.split("\t")))
											.mapToPair(f4 -> new Tuple2<String, Click>(f4.get(1), new Click(f4.get(0),
																											f4.get(1),
																											Integer.parseInt(f4.get(2)))));
		List<Tuple2<String, Tuple2<Register, Click>>> r = r1.join(r2).collect();
		
		for (Tuple2<String, Tuple2<Register, Click>> tuple2 : r) {
			Register regObj = tuple2._2._1;
			Click clkObj = tuple2._2._2;
			
			System.out.println("reg--->"+regObj);
			System.out.println("clk--->"+clkObj);
		}
		
		JavaRDD<Float> sum = reg.map(f1 -> Arrays.asList(f1.split("\t")))
		   .map(f2 -> (Float.parseFloat(f2.get(3))+Float.parseFloat(f2.get(4))));
		System.out.println(sum.collect());
		
		sc.close();
	}
}

@SuppressWarnings("serial")
class Register implements Serializable {
	String date, uuid;
	Integer cust_Id;
	Float lat, lng;
	
	Register(String date, String uuid, Integer cust_Id, Float lat, Float lng){
		this.date = date;
		this.uuid = uuid;
		this.cust_Id = cust_Id;
		this.lat = lat;
		this.lng = lng;
	}
	
	public String toString(){
		return this.date+"  "+this.uuid+"  "+this.cust_Id+"  "+this.lat+"  "+this.lng;
	}
}

@SuppressWarnings("serial")
class Click implements Serializable{
	String date,uuid;
	Integer landingPage;
	
	Click(String date, String uuid, Integer landingPage){
		this.date = date;
		this.uuid = uuid;
		this.landingPage = landingPage;
	}
	
	public String toString(){
		return this.date+"  "+this.uuid+"  "+this.landingPage;
	}
}
