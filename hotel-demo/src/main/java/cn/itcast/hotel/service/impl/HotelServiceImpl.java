package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelServiceImpl implements IHotelService {

    @Autowired
    private HotelMapper hotelMapper;

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    /**
     * ??????id ????????????????????????
     * @param id
     */
    @Override
    public void deleteById(Long id) {
        try {
            // 1.??????Request
            DeleteRequest request = new DeleteRequest("hotel", id.toString());
            // 2.????????????
            restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ??????id ??????es??????????????????
     * @param id
     */
    @Override
    public void insertById(Long id) {
        try {
            // 0.??????id??????????????????
            Hotel hotel = findById(id);
            // ?????????????????????
            HotelDoc hotelDoc = new HotelDoc(hotel);
            // 1.??????Request??????
            IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
            // 2.??????Json??????
            request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            // 3.????????????
            restHighLevelClient.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ??????????????????
     * @param prefix
     * @return
     */
    @Override
    public List<String> getSuggestions(String prefix) {

        try {
            // 1.??????Request ??????????????????
            SearchRequest request = new SearchRequest("hotel");
            // 2.??????DSL
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "suggestions",
                    SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(prefix)
                            .skipDuplicates(true)
                            .size(10)
            ));
            // 3.????????????
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            // 4.????????????
            Suggest suggest = response.getSuggest();
            // 4.1.?????????????????????????????????????????????
            CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
            // 4.2.??????options
            List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();
            // 4.3.??????
            List<String> list = new ArrayList<>(options.size());
            //??????????????????
            for (CompletionSuggestion.Entry.Option option : options) {
                String text = option.getText().toString();
                list.add(text);
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Hotel findById(Long id) {
        return hotelMapper.findById(id);
    }

    @Override
    public List<Hotel> findAll() {
        return hotelMapper.findAll();
    }

    @Override
    public PageResult search(RequestParams params) throws IOException {
        //0.??????RestClient??????
        //1. ??????????????????
        SearchRequest searchRequest = new SearchRequest("hotel");

        // ??????dsl??????
        buildBasicQuery(params,searchRequest);


        // 2.??????
        int page = params.getPage();
        int size = params.getSize();
        searchRequest.source().from((page-1)*size).size(size);

        // 2.3.??????
        String location = params.getLocation();
        if (location != null && !"".equals(location)) {
            searchRequest.source().sort(SortBuilders
                    .geoDistanceSort("location", new GeoPoint(location))
                    .order(SortOrder.ASC)
                    .unit(DistanceUnit.KILOMETERS)
            );
        }
        // 2. ???????????????Es
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 3. ??????????????????
        PageResult result = handleResponse(response);
        return result;


    }


    /**
     * ??????dsl??????
     * @param params
     * @param request
     */
    private void buildBasicQuery(RequestParams params, SearchRequest request) {
        // ????????? ??????Boolean??????
        // 1.??????BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 2.???????????????
        String key = params.getKey();
        if (key == null || "".equals(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        // 3.????????????
        if (params.getCity() != null && !params.getCity().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }
        // 4.????????????
        if (params.getBrand() != null && !params.getBrand().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }
        // 5.????????????
        if (params.getStarName() != null && !params.getStarName().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }
        // 6.??????
        if (params.getMinPrice() != null && params.getMaxPrice() != null) {
            boolQuery.filter(QueryBuilders
                    .rangeQuery("price")
                    .gte(params.getMinPrice())
                    .lte(params.getMaxPrice())
            );
        }
        //????????? ??????FunctionScoreQuery
        // ????????????
        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        // ???????????????????????????????????????
                        boolQuery,
                        // function score?????????
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                // ???????????????function score ??????
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        // ????????????
                                        QueryBuilders.termQuery("isAD", true),
                                        // ????????????
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });
        // 7.??????source
        request.source().query(functionScoreQuery);
    }

    /**
     * ??????????????????
     * @param response
     */
    public PageResult handleResponse(SearchResponse response) {
        // 3. ??????????????????
        // ?????????????????????????????????
        SearchHits hits = response.getHits();
        // ???????????????????????????
        long count = hits.getTotalHits().value;
        List<HotelDoc> list = new ArrayList<>();
        // ???????????????????????????
        SearchHit[] doc = hits.getHits();
        for (SearchHit hit : doc) {
            // ?????????????????????
            String sourceAsString = hit.getSourceAsString();
            //????????????
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            //??????????????? ??????????????????????????????
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0){
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            list.add(hotelDoc);
        }
        return new PageResult(count,list);
    }

    /**
     * ???????????????????????? ?????? ??????
     * @param params
     * @return
     */
    @Override
    public Map<String, List<String>> getFilters(RequestParams params) {
        try {
            // 1.??????Request
            SearchRequest request = new SearchRequest("hotel");
            // 2.??????DSL
            // 2.1.query
            // ?????????????????????????????????
            buildBasicQuery(params, request);
            // 2.2.??????size
            request.source().size(0);
            // 2.3.??????
            buildAggregation(request);
            // 3.????????????
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            // 4.????????????
            Map<String, List<String>> result = new HashMap<>();
            Aggregations aggregations = response.getAggregations();
            // 4.1.???????????????????????????????????????
            List<String> brandList = getAggByName(aggregations, "brandAgg");
            result.put("brand", brandList);
            // 4.2.???????????????????????????????????????
            List<String> cityList = getAggByName(aggregations, "cityAgg");
            result.put("city", cityList);
            // 4.3.???????????????????????????????????????
            List<String> starList = getAggByName(aggregations, "starAgg");
            result.put("starName", starList);

            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    private void buildAggregation(SearchRequest request) {
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(100)
        );
        request.source().aggregation(AggregationBuilders
                .terms("cityAgg")
                .field("city")
                .size(100)
        );
        request.source().aggregation(AggregationBuilders
                .terms("starAgg")
                .field("starName")
                .size(100)
        );
    }

    private List<String> getAggByName(Aggregations aggregations, String aggName) {
        // 4.1.????????????????????????????????????
        Terms brandTerms = aggregations.get(aggName);
        // 4.2.??????buckets
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        // 4.3.??????
        List<String> brandList = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            // 4.4.??????key
            String key = bucket.getKeyAsString();
            brandList.add(key);
        }
        return brandList;
    }
}
