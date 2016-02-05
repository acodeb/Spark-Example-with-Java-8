package com;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class WeatherWithSparkSQL {

	public static void main(String[] args) {
		SparkConf sConf = new SparkConf().setAppName("WeatherWithSparkSQL")
										 .setMaster("spark://Ankits-MacBook-Air.local:7077")
										 .set("spark.cores.max", "4")
										 .set("spark.deploy.defaultCores", "4");
		JavaSparkContext sc = new JavaSparkContext(sConf);
		SQLContext sqlContext = new SQLContext(sc);
		
		JavaRDD<String> file = sc.textFile("/Users/ankitjindal/Documents/Apache_Spark/Tutorials_and_Certification/Spark_offline_current/labfiles/nycweather/nycweather.csv");
		JavaRDD<Weather> weather = file.map(m -> Arrays.asList(m.split(",")))
													   .map(mp -> new Weather(mp.get(0), 
																					Integer.parseInt(mp.get(1).trim()),
																					Double.parseDouble(mp.get(2).trim())
																			)
														   );
		//weather.take(10).forEach(aa -> System.out.println(aa.date+"~~"+aa.temp+"~~"+aa.precip));
		
		DataFrame schemaWeather = sqlContext.createDataFrame(weather, Weather.class);
		schemaWeather.registerTempTable("weather");
		
		DataFrame precip = sqlContext.sql("Select date, temp, precip FROM weather where precip >= 0.0");
		precip.toJavaRDD().map(f -> "Date:"+f.get(0)+" Temp:"+f.get(1)+" Precip:"+f.get(2)).take(10).forEach(a -> System.out.println(a));

		DataFrame temp = sqlContext.sql("Select date, temp, precip FROM weather where temp <= 0");
		temp.toJavaRDD().map(f -> "Date:"+f.getString(0)+" Temp:"+f.getInt(1)+" Precip:"+f.getDouble(2)).take(10).forEach(a -> System.out.println(a));

		sc.close();
	}

	public static class Weather implements Serializable{

		private static final long serialVersionUID = 1L;
		private String date;
		private Integer temp;
		private Double precip;
		
		Weather(String date, Integer temp, Double precip){
			this.date = date;
			this.temp = temp;
			this.precip = precip;
		}

		public String getDate() {
			return date;
		}

		public void setDate(String date) {
			this.date = date;
		}

		public Integer getTemp() {
			return temp;
		}

		public void setTemp(Integer temp) {
			this.temp = temp;
		}

		public Double getprecip() {
			return precip;
		}

		public void setprecip(Double precip) {
			this.precip = precip;
		}
	}
}