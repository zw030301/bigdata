package com.zw.mapreduce.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 自定义序列化对象
 * @author zhengwei
 * @since 2018年11月20日 上午8:58:54
 */
public class FlowBean implements Writable {
	private long upFlow;//上行流量
	private long downFlow;//下行流量
	private long sumFlow;//总流量
	//无参构造器，反序列化时反射需要无参构造器
	public FlowBean() {
		super();
	}
	public FlowBean(long upFlow, long downFlow, long sumFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = sumFlow;
	}
	//序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}
	//反序列化方法
	@Override
	public void readFields(DataInput in) throws IOException {
		//反序列化的顺序必须和序列化的顺序保持一致
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();
	}
	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getDownFlow() {
		return downFlow;
	}
	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}
	public long getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}
	public void setSum(long upFlow2,long downFlow2) {
		upFlow=upFlow2;
		downFlow=downFlow2;
		sumFlow=upFlow2+downFlow2;
	}
}
