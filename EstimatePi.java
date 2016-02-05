package com;

import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class EstimatePi {

	public static void main(String[] args) {
		SparkConf sConf = new SparkConf().setAppName("EstimatePi").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sConf);
		
		int slices = 2;
		int n = 100000*slices;
		
		double count = sc.parallelize(Arrays.asList(n), slices)
		  .map(m ->{
			  double x = Math.random()*2-1;
			  double y = Math.random()*2-1;
			  return(x*x + y*y <1)?1:0;
				  
		  }).reduce((a,b) -> a+b);
		
		System.out.println("Pi is roughly " + BigDecimal.valueOf(4.0 * count / n).toPlainString());
		
		sc.close();
	}

}
