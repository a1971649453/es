package cn.itcast.hotel.demo2;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

@SpringBootTest
public class EsSearchTest {

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
                HttpHost.create("http://192.168.11.128:9200")
        ));
        System.out.println(client);
    }


    /**
     * match_all 查询所有
     * match 分词查询
     * multi_match 多字段分词查询
     * range 范围查询
     * term 精确查询
     * 算分函数查询 : 在案例中讲解
     * bool : 在案例中讲解
     * @throws IOException
     */
    @Test
    public void test01() throws IOException {
       //1. 创建数据请求
        SearchRequest searchRequest = new SearchRequest("hotel");
        //1.2.设置查询类型
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 构建类型 查询所有的DSL
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 1.3 将查询类型对象设置给请求语义对象
        searchRequest.source(searchSourceBuilder);
        // 2. 发送请求给Es
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 3. 获取响应结果
        handleResponse(response);


    }

    /**
     * match_all 查询所有
     * match 分词查询
     * multi_match 多字段分词查询
     * range 范围查询
     * term 精确查询
     * 算分函数查询 : 在案例中讲解
     * bool : 在案例中讲解
     * @throws IOException
     */
    @Test
    public void test02() throws IOException {
        //1. 创建数据请求
        SearchRequest searchRequest = new SearchRequest("hotel");
        //1.2.设置查询类型
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 构建类型
        // match 分词查询
//        searchSourceBuilder.query(QueryBuilders.matchQuery("name","如家外滩"));
        // multiMatch分词查询
//        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("如家外滩","name","brand"));
        // range 范围查询
//        searchSourceBuilder.query(QueryBuilders.rangeQuery("price").gte(100).lte(200));
        // term 词条查询
        searchSourceBuilder.query(QueryBuilders.termQuery("city","上海"));
        // 1.3 将查询类型对象设置给请求语义对象
        searchRequest.source(searchSourceBuilder);
        // 2. 发送请求给Es
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 3. 获取响应结果
        handleResponse(response);

    }


    /**
     * boolean 查询
     * @throws IOException
     */
    @Test
    public void test03() throws IOException {
        //1. 创建数据请求
        SearchRequest searchRequest = new SearchRequest("hotel");
        //1.2.设置查询类型
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //构建bool查询对象
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 设置必须满足的条件
        boolQueryBuilder.must(QueryBuilders.termQuery("city","上海"));
        // 设置过滤条件
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(2000));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("score").gte(45));

        // todo 将bool查询对象设置给查询类型对象
        searchSourceBuilder.query(boolQueryBuilder);
        // todo 将查询类型对象设置给请求语义对象
        searchRequest.source(searchSourceBuilder);
        // 2. 发送请求给Es
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 3. 获取响应结果
        handleResponse(response);

    }


    /**
     * 分页 排序
     * @throws IOException
     */
    @Test
    public void test04() throws IOException {
        //1. 创建数据请求
        SearchRequest searchRequest = new SearchRequest("hotel");
        //1.2.设置查询类型
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 排序
        searchSourceBuilder.sort("price", SortOrder.ASC);
        searchSourceBuilder.sort("score", SortOrder.ASC);
        // 分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        // 1.3 将查询类型对象设置给请求语义对象
        searchRequest.source(searchSourceBuilder);

        // 2. 发送请求给Es
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 3. 获取响应结果
        handleResponse(response);

    }

    /**
     * 结果加上标签 高亮
     * @throws IOException
     */
    @Test
    public void test05() throws IOException {
        //1. 创建数据请求
        SearchRequest searchRequest = new SearchRequest("hotel");
        //1.2.设置查询类型
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("all","如家酒店"));
        // 1.3 将查询类型对象设置给请求语义对象
        searchRequest.source(searchSourceBuilder);
        //todo 高亮查询
        searchRequest.source().highlighter(
                new HighlightBuilder().field("name").requireFieldMatch(false)

        );
        // 2. 发送请求给Es
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 3. 获取响应结果
        handleResponse(response);

    }


    /**
     * 解析相应结果
     * @param response
     */
    public void handleResponse(SearchResponse response) {
        // 3. 获取响应结果
        // 获取命中的所有数据信息
        SearchHits hits = response.getHits();
        // 获取命中的总记录数
        long value = hits.getTotalHits().value;
        System.out.println("总记录数:"+value);
        SearchHit[] doc = hits.getHits();
        for (SearchHit hit : doc) {
            //todo:解析高亮结果数据
            HighlightField highlightField = hit.getHighlightFields().get("name");
            // 获取文档的id
            String id = hit.getId();
            // 获取文档的内容
            String sourceAsString = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            if (highlightField != null){
                String name = highlightField.getFragments()[0].string();
                hotelDoc.setName(name);
            }
            System.out.println(hotelDoc);
        }
    }

    @AfterEach
    public void destory() throws IOException {
        if (client!=null){
            // 关闭客户端对象
            client.close();
        }
    }
}
