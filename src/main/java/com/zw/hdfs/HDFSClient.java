package com.zw.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;
/**
 * ����hadoop������Hadoop�ͻ���
 * @author zhengwei
 *
 */
public class HDFSClient {
	public static FileSystem getFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf=new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://slave02:9000"), conf, "zw");
		return fs;
	}
	/**
	 * �ж��Ƿ����ļ��������ļ���
	 * @throws URISyntaxException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	@Test
	public void testIsFile() throws IOException, InterruptedException, URISyntaxException {
		FileSystem fs=getFS();
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			if (fileStatus.isFile()) {
				System.out.println("f:"+fileStatus.getPath().getName());
			}else {
				System.out.println("d:"+fileStatus.getPath().getName());
			}
		}
	}
	/**
	 * �ļ�����鿴
	 * @throws URISyntaxException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	@Test
	public void testListFiles() throws IOException, InterruptedException, URISyntaxException {
		FileSystem fs=getFS();
		//�鿴�ļ��б�
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
		while(files.hasNext()) {
			LocatedFileStatus next = files.next();
			//
			System.out.println(next.getPath().getName());//�ļ�����
			System.out.println(next.getPermission());//Ȩ��
			System.out.println(next.getLen());//�ļ�����
			BlockLocation[] locations = next.getBlockLocations();//��ŵĿ���Ϣ
			for (BlockLocation blockLocation : locations) {
				String[] hosts = blockLocation.getHosts();//������
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("---------------------------------");
		}
		fs.close();
	}
	/**
	 * �ļ�����
	 * @throws URISyntaxException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	@Test
	public void testRenameFile() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf=new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://slave02:9000"), conf, "zw");
		fs.rename(new Path("/user/zw/input/wcinput.input"), new Path("/user/zw/input/wc.input"));
		fs.close();
	}
	/**
	 * ɾ���ļ�
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void testDeleteFile() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf=new Configuration();
		//1.��ȡhdfs����
		FileSystem fs = FileSystem.get(new URI("hdfs://slave02:9000"), conf, "zw");
		//ɾ���ļ�
		fs.delete(new Path("/user/zwzw"), true);//�Ƿ�ݹ�ɾ��
		fs.close();
	}
	/**
	 * �ļ�����
	 * @throws URISyntaxException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	@Test
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf=new Configuration();
		//1.��ȡhdfs����
		FileSystem fs = FileSystem.get(new URI("hdfs://slave02:9000"), conf, "zw");
		//�����ļ�
		fs.copyToLocalFile(new Path("/user/zw/input/wc.input"), new Path("C:\\Users\\zhengwei\\Desktop\\"));
		//3.�ر���Դ
		fs.close();
	}
	/**
	 * �ļ��ϴ�
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf=new Configuration();
		conf.set("", "");
		//1.��ȡhdfs����
		FileSystem fs = FileSystem.get(new URI("hdfs://slave02:9000"), conf, "zw");
		//2.ʹ��API�����ļ��ϴ�
		fs.copyFromLocalFile(new Path("C:\\Users\\zhengwei\\Desktop\\sss.txt")/*Դ·��*/, new Path("/user/zwzw/")/*Ŀ��·��*/);
		//3.�ر���Դ
		fs.close();
		System.out.println("over");
	}
	public static void main(String[] args) throws Exception {
		//������Ϣ
		Configuration conf=new Configuration();
//		conf.set("fs.defaultFS", "hdfs://slave02:9000");
		//1.��ȡHDFS����
//		FileSystem fs = FileSystem.get(conf);
		FileSystem fs = FileSystem.get(new URI("hdfs://slave02:9000"), conf, "zw");
		//2.��hdfs�ϴ���·��
		fs.mkdirs(new Path("/user/zwzw/"));
		//3.�ر���Դ
		fs.close();
		System.out.println("over");
	}
}
