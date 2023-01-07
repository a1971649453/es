package cn.itcast.hotel.demo3;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
public class AggsSearchTest {

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
        searchRequest.source().size(0);
        //1.2 聚合分桶 按照品牌分桶 品牌分桶中按照平均价格分桶
        searchRequest.source().aggregation(
                AggregationBuilders.terms("brandName")
                        .field("brand")
                        .size(10).subAggregation(
                                AggregationBuilders.avg("avgPrice")
                                        .field("price")
                        )
        );
        // 根据城市分桶
        searchRequest.source().aggregation(
                AggregationBuilders.terms("cityName")
                        .field("city")
                        .size(10)
        );


        // 2. 发送请求给Es
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // 3. 获取响应结果
        // 解析结果
        handleResponse(response,"brandName");
//        Aggregations aggregations = response.getAggregations();
//        // 获取其中的某一个聚合结果
//        Terms termsAgg1 = aggregations.get("brandName");
//        // 获取所有的桶信息
//        List<? extends Terms.Bucket> buckets1 = termsAgg1.getBuckets();
//        for (Terms.Bucket bucket : buckets1) {
//            System.out.println("品牌名称:"+bucket.getKeyAsString());
//            System.out.println("品牌数量:"+bucket.getDocCount());
//            // 获取子聚合结果
//            Aggregations subAggregations = bucket.getAggregations();
//            // 获取子聚合中的avg聚合结果
//            ParsedAvg avgPrice = subAggregations.get("avgPrice");
//            // 获取子聚合中的桶信息
//            System.out.println("品牌平均价格:"+avgPrice.getValue());

//        }
        System.out.println("=====================================");
        handleResponse(response,"cityName");
        //==============
        // 获取其中的某一个聚合结果
//        Terms termsAgg2 = aggregations.get("cityName");
//        // 获取所有的桶信息
//        List<? extends Terms.Bucket> buckets2 = termsAgg2.getBuckets();
//        for (Terms.Bucket bucket : buckets2) {
//            System.out.println("品牌名称:"+bucket.getKeyAsString());
//            System.out.println("品牌数量:"+bucket.getDocCount());

        }






    /**
     * 解析响应结果数据
     * @param response
     */
    public void handleResponse(SearchResponse response,String name){
        //1.获取所有的聚合结果
        Aggregations aggregations = response.getAggregations();
        //2.根据聚合名称获取对应的聚合结果
        Terms terms = aggregations.get(name);
        //3.获取桶数组
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            Object key = bucket.getKey();
            long count = bucket.getDocCount();
            System.out.println(key+" : "+count);
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
