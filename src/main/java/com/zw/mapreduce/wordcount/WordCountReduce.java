package com.zw.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	IntWritable v=new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		//zw,1
		//zw,2
		//key=zw,values=[1,1]
		//累加求和
		int sum=0;
		for (IntWritable value : values) {
			sum+=value.get();
		}
		v.set(sum);
		//写出:zw,2
		context.write(key, v);//key的内容是相同的，value的内容是累加的和
	}
}
