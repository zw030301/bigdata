package com.zw.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * 
 * @author  zhengwei(���ţ�79505)
 * @version 1.0
 * @since 2018��11��9�� ����5:28:42
 * @category com.zw.spark
 * @copyright �Ͼ���������Ƽ���˾
 *
 */
public class SparkSQL {
	public static void main(String[] args) {
		SparkSession saprk=SparkSession.builder().appName("sparkSQL").config("spark.some.config.option","some-value").getOrCreate();
	}
}
