package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {
            //准备request
            SearchRequest request = new SearchRequest("hotel");
            //准备DSL
            //query
            buildBaiscQuery(params, request);

            //地理位置排序
            String location = params.getLocation();
            if(location!=null&&!"".equals(location.trim())){
                request.source().sort(SortBuilders.geoDistanceSort("location",
                        new GeoPoint(location)).order(SortOrder.ASC).unit(DistanceUnit.KILOMETERS));
            }
            //分页
            int page=params.getPage();
            int size=params.getSize();
            request.source().from((page-1)*size).size(size);
            SearchResponse search = client.search(request, RequestOptions.DEFAULT);
            return handleResponse(request);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filters(RequestParams params) {
        try {
            SearchRequest request = new SearchRequest("hotel");
            request.source().size(0);
            //query 限定查询范围
            buildBaiscQuery(params, request);

            //聚合
            bulidAggregation(request);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            Map<String, List<String>> result = new HashMap<>();
            //解析结果
            //获取聚合觉果
            Aggregations aggregations = response.getAggregations();
            //根据名称获取结果
            List<String> brandList = getAggByName(aggregations,"brandAgg");
            result.put("品牌",brandList);
            List<String> cityList = getAggByName(aggregations,"cityAgg");
            result.put("城市",cityList);
            List<String> starList = getAggByName(aggregations,"starAgg");
            result.put("星级",starList);
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * /
     * @param aggregations 聚合结果
     * @param aggName 聚合名称
     * @return 返回聚合后结果List
     */
    private List<String> getAggByName(Aggregations aggregations,String aggName) {
        Terms brandTerms = aggregations.get(aggName);
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        List<String> brandList = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            brandList.add(key);
        }
        return brandList;
    }

    private void bulidAggregation(SearchRequest request) {
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg").field("brand").size(100));
        request.source().aggregation(AggregationBuilders
                .terms("cityAgg").field("city").size(100));
        request.source().aggregation(AggregationBuilders
                .terms("starAgg").field("starName").size(100));
    }

    //构建bolleanQuery
    private void buildBaiscQuery(RequestParams params, SearchRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //must关键字搜索
        String key = params.getKey();
        if(key==null||"".equals(key.trim())){
            boolQuery.must(QueryBuilders.matchAllQuery());
        }else {
            boolQuery.must(QueryBuilders.matchQuery("all",key));
        }
        //城市
        if(params.getCity()!=null&&!"".equals(params.getCity().trim())){
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }
        //品牌
        if(params.getBrand()!=null&&!"".equals(params.getBrand ().trim())){
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }
        //星级
        if(params.getStarName()!=null&&!"".equals(params.getStarName ().trim())){
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }
        if(params.getMinPrice()!=null&& params.getMaxPrice()!=null){
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(params.getMinPrice())
                    .lte(params.getMaxPrice()));
        }

        //算分控制
        FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(
                boolQuery,//原始查询  相关性算分
                //function score 的数组
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                        //其中一个function score元素
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                //过滤条件
                                QueryBuilders.termQuery("isAD", true),
                                //算分函数
                                ScoreFunctionBuilders.weightFactorFunction(10)
                        )
                });
        request.source().query(functionScoreQuery);
    }


    private PageResult handleResponse(SearchRequest request) throws IOException {
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析结果
        SearchHits searchHits = response.getHits();
        //4.1查询条数
        long value = searchHits.getTotalHits().value;
        //4.2获取数组
        SearchHit[] hits = searchHits.getHits();
        //4.3遍历hits
        ArrayList<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            //4.4获取souce
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //获取高亮结果
            //获取排序值
            Object[] sortValues = hit.getSortValues();
            if(sortValues.length>0){
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotels.add(hotelDoc);

        }
        return new PageResult(value,hotels);
    }
}
