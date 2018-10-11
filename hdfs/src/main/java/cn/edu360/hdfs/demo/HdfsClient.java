package cn.edu360.hdfs.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;

public class HdfsClient {
    public static void main(String[] args) throws Exception {
        /**
         * Configuration参数对象的机制：
         *    构造时，会加载jar包中的默认配置 xx-default.xml
         *    再加载 classpath下用户配置xx-site.xml  ，覆盖掉默认参数
         *    构造完成之后，还可以conf.set("p","v")，会再次覆盖用户配置文件中的参数值
         */
        // new Configuration()会从项目的classpath中加载core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml等文件
        Configuration conf = new Configuration();
        // 指定本客户端上传文件到hdfs时需要保存的副本数为：2
        conf.set("dfs.replication", "2");
        // 指定本客户端上传文件到hdfs时切块的规格大小：64M
        conf.set("dfs.blocksize", "16m");
        // 构造一个访问指定HDFS系统的客户端对象: 参数1:——HDFS系统的URI，参数2：——客户端要特别指定的参数，参数3：客户端的用户名，如果不填，会从windows系统自动取一个
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.245.128:9000/"), conf, "root");
        // 本地上传一个文件到HDFS的input目录中并将文件改名为 abc.txt；srcPath to dstPath
        fs.copyFromLocalFile(new Path("D:/123.txt"), new Path("/input/abc.txt"));
        fs.close();
    }

    FileSystem fs = null;

    @Before
    public void init() throws Exception{
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.blocksize", "64m");
        fs = FileSystem.get(new URI("hdfs://192.168.245.128:9000/"), conf, "root");
    }

    /**从HDFS中下载文件到客户端本地磁盘*/
    @Test
    public void testGet() throws IllegalArgumentException, IOException {
        /*将hdfs上的文件下载到windows，hdfs客户端要访问win本地磁盘，要调用hadoop中的c语言库，如果调不到就会报错，因此需要在win中设置HADOOP_HOME*/
        fs.copyToLocalFile(new Path("/input/abc.txt"), new Path("f:/123.txt"));
        fs.close();
    }

    /**在hdfs内部移动文件，从一个hdfs路径到另一个hdfs路径*/
    @Test
    public void testRename() throws Exception{
        /*fs.rename()修改文件路径和文件名称*/
        fs.rename(new Path("/input/abc.txt"), new Path("/output/result.txt"));
        fs.close();
    }

    /**在hdfs中创建文件夹；命令行 ./hadoop fs -mkdir -p /gouyan/hadoop*/
    @Test
    public void testMkdir() throws Exception{
        fs.mkdirs(new Path("/input"));
        fs.close();
    }


    /**在hdfs中删除文件或文件夹，true表示递归删除*/
    @Test
    public void testRm() throws Exception{
        fs.delete(new Path("/aaa"), true);
        fs.close();
    }

    /**查询hdfs指定目录下的文件信息*/
    @Test
    public void testLs() throws Exception{
        /*fs.listFiles()列出指定路径下的所有文件的描述信息，true表示递归列出子目录中文件描述信息；对于hdfs来说，只要能把文件copy过来就可以，不关系文件内容*/
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true);
        while(iter.hasNext()){
            LocatedFileStatus status = iter.next();
            System.out.println("文件全路径："+status.getPath());
            System.out.println("块大小："+status.getBlockSize());
            System.out.println("文件长度："+status.getLen());
            System.out.println("副本数量："+status.getReplication());
            System.out.println("块信息："+Arrays.toString(status.getBlockLocations()));
            System.out.println("--------------------------------");
        }
        fs.close();
    }

    /**查询hdfs指定目录下的文件和文件夹信息*/
    @Test
    public void testLs2() throws Exception{
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for(FileStatus status:listStatus){
            System.out.println("文件全路径："+status.getPath());
            System.out.println(status.isDirectory()?"这是文件夹":"这是文件");
            System.out.println("块大小："+status.getBlockSize());
            System.out.println("文件长度："+status.getLen());
            System.out.println("副本数量："+status.getReplication());
            System.out.println("--------------------------------");
        }
        fs.close();
    }
    /**读取hdfs中的文件的内容*/
    @Test
    public void testReadData() throws IllegalArgumentException, IOException {
        /*通过fs对象获取一个输入流 FSDataInputStream*/
        FSDataInputStream in = fs.open(new Path("/input/abc.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
        String line = null;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();in.close();fs.close();
    }

    /**读取hdfs中文件的指定偏移量范围的内容;作业题：用本例中的知识，实现读取一个文本文件中的指定BLOCK块中的所有数据*/
    @Test
    public void testRandomReadData() throws IllegalArgumentException, IOException {
        FSDataInputStream in = fs.open(new Path("/xx.dat"));
        // 将读取的起始位置进行指定
        in.seek(12);
        // 读16个字节
        byte[] buf = new byte[16];
        in.read(buf);
        System.out.println(new String(buf));
        in.close();fs.close();
    }

    /**往hdfs中的文件写内容*/
    @Test
    public void testWriteData() throws IllegalArgumentException, IOException {
        FSDataOutputStream out = fs.create(new Path("/zz.jpg"), false);
        // D:\images\006l0mbogy1fhehjb6ikoj30ku0ku76b.jpg
        FileInputStream in = new FileInputStream("D:/images/006l0mbogy1fhehjb6ikoj30ku0ku76b.jpg");
        byte[] buf = new byte[1024];
        int read = 0;
        while ((read = in.read(buf)) != -1) {
            out.write(buf,0,read);
        }
        in.close();out.close();fs.close();
    }
}
