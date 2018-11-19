package com.zw.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

/**
 * 
 * @author  zhengwei(���ţ�79505)
 * @version 1.0
 * @since 2018��11��12�� ����9:07:58
 * @category com.zw.spark
 * @copyright �Ͼ���������Ƽ���˾
 *
 */
public class SparkRDD {
	@Test
	public void Test1() {
		SparkConf conf=new SparkConf().setAppName("testRDD").setMaster("local[2]");
		JavaSparkContext sc=new JavaSparkContext(conf);
		List<Integer> data=Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> dataRDD = sc.parallelize(data);
		List<Integer> collect = dataRDD.collect();
		System.out.println(collect.toString());
	}
	@Test
	public void TestRDDTrans() {
		SparkConf conf=new SparkConf().setAppName("RDD").setMaster("local[2]");
		System.setProperty("hadoop.home.dir", "C:\\Users\\zhengwei\\Documents\\GitHub\\hadoop-common-2.6.0-bin");
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("C:\\Users\\zhengwei\\Desktop\\work.txt");
		JavaRDD<Integer> lineLength = lines.map(line->line.length());
		Integer totalLength = lineLength.reduce((x,y)->x+y);
		System.out.println("===============================");
		System.out.println(totalLength);
	}
}
