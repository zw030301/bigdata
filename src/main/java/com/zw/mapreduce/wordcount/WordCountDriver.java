package com.zw.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * wordcount驱动类
 * @author zhengwei
 *
 */
public class WordCountDriver {
	public static void main(String[] args) {
		try {
			Configuration conf=new Configuration();
			//1.获取job对象
			Job job=Job.getInstance(conf);
			//2.设置jar存储位置
			job.setJarByClass(WordCountDriver.class);//通过反射机制找到jar的位置
			//3.关联Mapper和Reduce
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReduce.class);
			//4.设置Mapper阶段输出数据的KV类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			//5.设置最终数据的KV类型
			job.setOutputKeyClass(Text.class);
			job.setOutputKeyClass(IntWritable.class);
			//如果不设置InputFormat,他默认用的是TextInputFormat.class
			job.setInputFormatClass(CombineFileInputFormat.class);
			//设置虚拟存储切片的最大值，为4m
			CombineFileInputFormat.setMaxInputSplitSize(job, 4190304);
			//6.设置程序的输入路径和输出路径
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			//7.提交job
//			job.submit();
			boolean result = job.waitForCompletion(true);
			System.exit(result?0:1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
