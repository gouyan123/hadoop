package cn.edu360.es;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;

/**简单查询*/
public class HelloWorld {
    public static void main(String[] args) {
        try {
            /**设置集群名称
             * 集群名称通过 http://192.168.245.128:9200/获取；*/
            Settings settings = Settings.builder()
                    .put("cluster.name", "elasticsearch")
                    .build();
            //创建client
            TransportClient client = new PreBuiltTransportClient(settings).addTransportAddresses(
                    //用java访问ES用的端口是9300
                    new InetSocketTransportAddress(InetAddress.getByName("192.168.245.128"), 9300));
//                    new InetSocketTransportAddress(InetAddress.getByName("192.168.245.128"), 9300),
//                    new InetSocketTransportAddress(InetAddress.getByName("192.168.245.128"), 9300));
           /** prepareGet(...)表示搜索数据；
            * execute()表示执行查询命令，将结果存到future里，但是不一定什么时候返回结果，相当于Transformation转换算子；
            * actionGet()是同步方法，去future里面等返回结果，没有返回就等待，相当于action算子；*/
            GetResponse response = client.prepareGet("store", "books", "1").execute().actionGet();
            //输出结果
            System.out.println(response);
            //关闭client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
