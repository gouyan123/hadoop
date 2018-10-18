package cn.edu360.mr.wc;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * job的客户端程序，用于提交mapreduce程序的jar包到 yarn集群；
 * 功能：
 *   1、封装本次job运行时所需要的必要参数
 *   2、跟yarn进行交互，将mapreduce程序成功的启动、运行
 */
public class JobSubmitter {
	public static void main(String[] args) throws Exception {
		// 在代码中设置JVM系统参数，用于给job对象来获取访问HDFS的用户身份
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
		// 1、设置job运行时要访问的默认文件系统
		conf.set("fs.defaultFS", "hdfs://h250:9000");
		// 2、设置job提交到哪去运行；1 在yarn上运行；2 在本地运行；
		conf.set("mapreduce.framework.name", "yarn");
		// 设置 yarn的 Resource Manager节点的 hostname；
		conf.set("yarn.resourcemanager.hostname", "h250");
		// 3、如果要从windows系统上运行这个job提交客户端程序，则需要加这个跨平台提交的参数
		conf.set("mapreduce.app-submission.cross-platform","true");
		// 根据配置获取 job对象
		Job job = Job.getInstance(conf);
		// 1、job中封装参数：jar包所在的位置
//		job.setJar("d:/wc.jar");	// 这种写死了，一般不用
		// 通过JobSubmitter.class找到它的包的路径；
		job.setJarByClass(JobSubmitter.class);
		// 2、封装参数： 本次job所要调用的Mapper实现类、Reducer实现类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		// 3、封装参数：本次job的Mapper实现类、Reducer实现类产生的结果数据的key、value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Path output = new Path("/wordcount/output");
		FileSystem fs = FileSystem.get(new URI("hdfs://h250:9000"),conf,"root");
		if(fs.exists(output)){
			fs.delete(output, true);
		}
		// 4、封装参数：本次job要处理的输入数据集所在路径、最终结果的输出路径
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
		FileOutputFormat.setOutputPath(job, output);  // 注意：输出路径必须不存在
		// 5、封装参数：想要启动的reduce task的数量；map task数量由系统分配；
		job.setNumReduceTasks(2);
		// 6、提交job给yarn，mapreduce程序在 yarn中运行，客户端会与mapreduce程序失去联系；因此使用 job.waitForCompletion()方法，表示客户端一直等待，不退出；
		// true表示将 ResoureManager返回的信息打印出来；
		boolean res = job.waitForCompletion(true);
		// res = true表示mapreduce在yarn集群上面运行成功，则res?0:-1返回0，System.exit(0)表示程序正常退出，System.exit(-1)表示程序异常退出；
		System.exit(res?0:-1);
	}
}
