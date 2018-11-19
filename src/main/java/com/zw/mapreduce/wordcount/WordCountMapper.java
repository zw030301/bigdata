package com.zw.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Mapper阶段
 * KEYIN:输入的key值类型，文件内容的偏移量
 * VALUEIN:输入的value值类型
 * KEYOUT:输出的key值类型
 * VALUEOUT:输出的value值类型
 * @author zhengwei
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text k=new Text();
	IntWritable v=new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//zw zw 
		//获取一行
		String line = value.toString();
		//切割一行["zw","zw"]
		String[] words = line.split(" ");
		//循环遍历写出
		for (String word : words) {
			//k=zw
			k.set(word);
			//v=1
			context.write(k, v);
		}
	}
}
