package com.zw.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import scala.Tuple2;

/**
 * 
 * @author  zhengwei(工号：79505)
 * @version 1.0
 * @since 2018年11月8日 上午9:06:34
 * @category com.zw.spark
 * @copyright 南京亚信网络科技公司
 *
 */
public class SparkStreaming {
	@SuppressWarnings("resource")
	@Test
	public void testSparkStreamingSocket() throws InterruptedException {
		SparkConf conf=new SparkConf().setAppName("spark_streaming").setMaster("local[2]");
		JavaStreamingContext jssc=new JavaStreamingContext(conf,Durations.seconds(1));
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost",9999);
		JavaDStream<String> words = lines.flatMap(line->Arrays.asList(line.split(" ")).iterator());
		JavaPairDStream<String, Integer> paris = words.mapToPair(word->new Tuple2<>(word, 1));
		JavaPairDStream<String, Integer> wordCounts = paris.reduceByKey((i1,i2)->i1+i2);
		wordCounts.print();
		jssc.start();
		jssc.awaitTermination();
	}
}
