package com.zw.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * 
 * @author  zhengwei(工号：79505)
 * @version 1.0
 * @since 2018年11月9日 下午5:28:42
 * @category com.zw.spark
 * @copyright 南京亚信网络科技公司
 *
 */
public class SparkSQL {
	public static void main(String[] args) {
		SparkSession saprk=SparkSession.builder().appName("sparkSQL").config("spark.some.config.option","some-value").getOrCreate();
	}
}
