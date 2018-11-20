package com.zw.mapreduce.serialization;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	FlowBean v=new FlowBean();
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		//1累加求和
		long sum_upFlow=0;
		long sum_downFlow=0;
		for (FlowBean value : values) {
			sum_upFlow+=sum_upFlow;
			sum_downFlow+=sum_downFlow;
		}
		v.setSum(sum_upFlow, sum_downFlow);
		//2写出
		context.write(key, v);
	}

}
