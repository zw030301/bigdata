package com.zw.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 
 * @author  zhengwei(工号：79505)
 * @version 1.0
 * @since 2018年11月16日 上午8:53:10
 * @category com.zw.spark
 * @copyright 南京亚信网络科技公司
 *
 */
public class SparkWordCount {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf().setAppName("SparkWordCount").setMaster("local[2]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("src/main/resources/input/word.zw");
		JavaRDD<String[]> map = lines.map(line->line.split(" "));
		System.out.println(map.collect().toString());
		JavaRDD<String> words = lines.flatMap(line->Arrays.asList(line.split(" ")).iterator());
		JavaPairRDD<String, Integer> ones = words.mapToPair(word->new Tuple2<>(word, 1));
		JavaPairRDD<String,Integer> count = ones.reduceByKey((x1,x2)->x1+x2);
		List<Tuple2<String,Integer>> collect = count.collect();
		for (Tuple2<String, Integer> tuple2 : collect) {
			System.out.println(tuple2._1()+"---"+tuple2._2());
		}
	}
}
