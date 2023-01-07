//package cn.itcast.hotel.service.impl;
//
//import cn.itcast.hotel.mapper.HotelMapper;
//import cn.itcast.hotel.pojo.Hotel;
//import cn.itcast.hotel.pojo.HotelDoc;
//import cn.itcast.hotel.pojo.PageResult;
//import cn.itcast.hotel.pojo.RequestParams;
//import cn.itcast.hotel.service.IHotelService;
//import com.alibaba.fastjson.JSON;
//import org.elasticsearch.action.search.SearchRequest;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.search.SearchHit;
//import org.elasticsearch.search.SearchHits;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//@Service
//public class HotelServiceImplBakend implements IHotelService {
//
//    @Autowired
//    private HotelMapper hotelMapper;
//
//    @Autowired
//    private RestHighLevelClient restHighLevelClient;
//
//    @Override
//    public Hotel findById(Long id) {
//        return hotelMapper.findById(id);
//    }
//
//    @Override
//    public List<Hotel> findAll() {
//        return hotelMapper.findAll();
//    }
//
//    @Override
//    public PageResult search(RequestParams params) throws IOException {
//        //0.创建RestClient对象
//        //1. 创建数据请求
//        SearchRequest searchRequest = new SearchRequest("hotel");
//
//        //1.2.设置查询类型
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        String key = params.getKey();
//        Integer page = params.getPage();
//        Integer size = params.getSize();
//        if (key == null || "".equals(key)){
//            //用户没有输入搜索内容
//            // 构建类型 查询所有的DSL
//            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
//
//        }else {
//            searchSourceBuilder.query(QueryBuilders.matchQuery("all",key));
//        }
//
//        //  todo 分页
//        searchSourceBuilder.from((page -1) * size);
//        searchSourceBuilder.size(size);
//        // 1.3 将查询类型对象设置给请求语义对象
//        searchRequest.source(searchSourceBuilder);
//
//        // 2. 发送请求给Es
//        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        // 3. 获取响应结果
//        PageResult result = handleResponse(response);
//        return result;
//
//
//    }
//
//
//
//    /**
//     * 解析相应结果
//     * @param response
//     */
//    public PageResult handleResponse(SearchResponse response) {
//        // 3. 获取响应结果
//        // 获取命中的所有数据信息
//        SearchHits hits = response.getHits();
//        // 获取命中的总记录数
//        long count = hits.getTotalHits().value;
//        List<HotelDoc> list = new ArrayList<>();
//        // 将数据放入到集合中
//        SearchHit[] doc = hits.getHits();
//        for (SearchHit hit : doc) {
//            // 获取文档的id
//            String id = hit.getId();
//            // 获取文档的内容
//            String sourceAsString = hit.getSourceAsString();
//            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
//            list.add(hotelDoc);
//        }
//        return new PageResult(count,list);
//    }
//}
