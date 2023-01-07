package cn.itcast.hotel.demo1;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

@SpringBootTest
public class DocRestClientTest {

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
     * 添加文档数据
     */
    @Test
    public void test01() throws IOException {
        IndexRequest request = new IndexRequest("heima");
        //设置文档id id要求唯一 如果不设置id es会生成随机id
        request.id("1");
        String docJson = "{\n" +
                "  \"info\": \"黑马程序员Java讲师\",\n" +
                "  \"email\":\"zy@itcast.cn\",\n" +
                "  \"name\":{\n" +
                "    \"firstName\":\"云\",\n" +
                "    \"lastName\":\"赵\"\n" +
                "  },\n" +
                "  \"age\": 38\n" +
                "}";
        request.source(docJson, XContentType.JSON);
        //发送请求
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        //3.处理相应结果
        DocWriteResponse.Result result = response.getResult();
        System.out.println("result = " + result);

    }

    /**
     * 查看文档数据
     * @throws IOException
     */
    @Test
    public void test02() throws IOException {
        GetRequest request = new GetRequest("heima");
        //设置文档id id要求唯一 如果不设置id es会生成随机id
        request.id("1");
        try {
            //发送请求
            GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
            String sourceAsString = getResponse.getSourceAsString();
            System.out.println(sourceAsString);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                System.out.println("文档不存在");
            }
        }
    }

    /**
     * 修改文档
     * @throws IOException
     */
    @Test
    public void test03() throws IOException {
        UpdateRequest request = new UpdateRequest("heima", "1");
        String jsonString = "{\n" +
                "  \"info\": \"黑马程序员Java讲师\",\n" +
                "  \"email\":\"qq@itcast.cn\",\n" +
                "  \"name\":{\n" +
                "    \"firstName\":\"云\",\n" +
                "    \"lastName\":\"赵\"\n" +
                "  },\n" +
                "  \"age\": 38\n" +
                "}";
        request.doc(jsonString, XContentType.JSON);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        DocWriteResponse.Result result = response.getResult();
        System.out.println(result);
    }

    /**
     * 修改文档
     * @throws IOException
     */
    @Test
    public void test04() throws IOException {
        UpdateRequest request = new UpdateRequest("heima", "1");
        request.doc("email", "zhangsan@qq.com");
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        DocWriteResponse.Result result = response.getResult();
        System.out.println(result);
    }

    /**
     * 删除文档
     * @throws IOException
     */
    @Test
    public void test05() throws IOException {
        DeleteRequest request = new DeleteRequest("heima", "1");

        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        DocWriteResponse.Result result = response.getResult();
        System.out.println(result);
    }



    @AfterEach
    public void destory() throws IOException {
        if (client!=null){
            // 关闭客户端对象
            client.close();
        }
    }
}
