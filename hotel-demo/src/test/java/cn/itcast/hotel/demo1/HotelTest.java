package cn.itcast.hotel.demo1;

import cn.itcast.hotel.constants.HotelConstants;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

@SpringBootTest
public class HotelTest {

    private RestHighLevelClient client = null;

    @Resource
    private IHotelService iHotelService;


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
                HttpHost.create("http://192.168.11.128:9200")
        ));
        System.out.println(client);
    }

    /**
     * 创建索引库
     */
    @Test
    public void createHotel() throws IOException {
        //1.创建请求语义对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        //发送请求给ES
        //设置映射关系
        request.mapping(HotelConstants.MAPPING_TEMPLATE,XContentType.JSON);
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);

        //处理相应结果
        if (response.isAcknowledged()){
            System.out.println("创建索引库成功");
        }
    }

    /**
     * 将mysql中的数据导入到ES中
     */
    @Test
    public void addDocHotel() throws IOException {
        //从mysql中查询酒店数据
        Hotel hotel = iHotelService.findById(36934L);
        //将Java对象转为字符串
        //将Hotel转为HotelDoc
        HotelDoc hotelDoc = new HotelDoc(hotel);
        String hotelDocJson = JSON.toJSONString(hotelDoc);
        System.out.println(hotel);
        //1.创建请求语义对象
        IndexRequest request = new IndexRequest("hotel");
        //设置文档id
        request.id("36934");
        request.source(hotelDocJson,XContentType.JSON);
        //发送请求给ES
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        //处理相应结果
        DocWriteResponse.Result result = response.getResult();
        System.out.println(result);

    }


    /**
     * 将mysql中的数据批量导入到ES中
     */
    @Test
    public void bulkHotel() throws IOException {
        //从mysql中查询酒店数据
        List<Hotel> list = iHotelService.findAll();
        BulkRequest bulkRequest = new BulkRequest();
        //批量处理
        for (Hotel hotel : list) {
            //将Java对象转为字符串
            //将Hotel转为HotelDoc
            HotelDoc hotelDoc = new HotelDoc(hotel);
            String hotelDocJson = JSON.toJSONString(hotelDoc);
            IndexRequest request = new IndexRequest("hotel");
            request.id(hotel.getId()+"");
            request.source(hotelDocJson,XContentType.JSON);
            bulkRequest.add(request);
        }
        //发送请求给ES
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        //处理相应结果
        System.out.println(response.status());

    }


    @AfterEach
    public void destory() throws IOException {
        if (client!=null){
            // 关闭客户端对象
            client.close();
        }
    }


}
