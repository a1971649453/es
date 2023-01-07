package cn.itcast.hotel.demo1;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

@SpringBootTest
public class HelloRestClientTest {

    private RestHighLevelClient client = null;
    /**
     * 创建RestClient对象,操作ES
     */
    @BeforeEach
    public void init() throws IOException {
        //1.创建RestClient对象用于操作ES,向ES发送rest风格的请求
//        RestHighLevelClient client = new RestHighLevelClient(
//                RestClient.builder(
//                        //new HttpHost("localhost", 9200, "http"),
//                        new HttpHost("192.168.163.141", 9200, "http")));
        client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.113.102:9200")
        ));
        System.out.println(client);
    }


    /**
     * 通过RestClient对象操作 向ES发送请求 实现索引库的创建
     */
    @Test
    public void test01() throws IOException {
        //0.创建RestClient对象
        //1.指定想要什么操作 创建heima索引库
        CreateIndexRequest request = new CreateIndexRequest("heima");
        //添加属性映射
        //本次创建索引库需要添加的属性映射
        request.mapping("{\n" +
                "    \"properties\": {\n" +
                "      \"info\": {\n" +
                "        \"type\": \"text\",\n" +
                "        \"index\": true,\n" +
                "        \"analyzer\": \"ik_max_word\"\n" +
                "      },\n" +
                "      \"email\":{\n" +
                "        \"type\": \"keyword\",\n" +
                "        \"index\": false\n" +
                "      },\n" +
                "      \"name\":{\n" +
                "        \"properties\": {\n" +
                "          \"firstName\":{\n" +
                "            \"type\": \"text\",\n" +
                "            \"index\": false\n" +
                "          },\n" +
                "          \"lastName\":{\n" +
                "            \"type\": \"text\",\n" +
                "            \"index\": false\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }", XContentType.JSON);

        //2.发送请求,并接收响应
        // 参数1:请求语义对象 参数2:请求方式(默认)
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = createIndexResponse.isAcknowledged();
        //3.处理响应结果
        System.out.println("acknowledged = " + acknowledged);

    }

    @Test
    public void test03() throws IOException {
        //0.创建RestClient对象
        //1.指定想要什么操作 删除heima索引库
        DeleteIndexRequest request = new DeleteIndexRequest("heima");

        //2.发送请求,并接收响应
        // 参数1:请求语义对象 参数2:请求方式(默认)
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        boolean acknowledged = delete.isAcknowledged();
        //3.处理响应结果
        System.out.println("acknowledged = " + acknowledged);

    }

    /**
     * 判断索引库是否存在
     *
     * @throws IOException
     */
    @Test
    public void test04() throws IOException {
        //0.创建RestClient对象
        //1.指定想要什么操作 创建heima索引库
        GetIndexRequest request = new GetIndexRequest("heima1");

        //2.发送请求
        boolean flag = client.indices().exists(request, RequestOptions.DEFAULT);

        if (flag){
            System.out.println("索引库存在");
        }else
            System.out.println("索引库不存在");


    }

    @Test
    public void test05() throws IOException {
        GetIndexRequest request = new GetIndexRequest("heima");
        GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
        Map<String, MappingMetadata> map = response.getMappings();
        MappingMetadata metadata = map.get("heima");
        Map<String, Object> mappig = metadata.getSourceAsMap();
        System.out.println(mappig);
    }


    @AfterEach
    public void destory() throws IOException {
        if (client!=null){
            // 关闭客户端对象
            client.close();
        }
    }
}
