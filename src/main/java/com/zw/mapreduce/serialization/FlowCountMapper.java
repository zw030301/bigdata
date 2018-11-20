package com.zw.mapreduce.serialization;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	Text k=new Text();
	FlowBean v=new FlowBean();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		//1	18851865961	127.0.0.1	500	500	200
		String line = value.toString();//获取文件的每一行
		String[] words = line.split("\t");//分割每一行的数据
		k.set(words[2]);//手机号
		v.setUpFlow(Long.parseLong(words[words.length-2]));//上行流量
		v.setDownFlow(Long.parseLong(words[words.length-3]));//下行流量
		context.write(k, v);
	}
}
