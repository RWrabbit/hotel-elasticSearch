package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.similarity.ScriptedSimilarity;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@SpringBootTest
public class HotelSearchTest {
    private RestHighLevelClient client;
    @Autowired
    private IHotelService hotelService;

    @Test
    void testMatchAll() throws IOException {
        //1.准备request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备dsl
        request.source().query(QueryBuilders.matchAllQuery());
        //3.发送请求
        handleResponse(request);
    }

    @Test
    void testHightlight() throws IOException {
        //1.准备request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备dsl
        request.source().query(QueryBuilders.matchQuery("all","如家")).
                highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        //3.发送请求
        handleResponse(request);
    }

    @Test
    void testMatch() throws IOException {
        //模拟前端传入的页码
        int page =2; int size=5;
        //1.准备request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备dsl
        request.source().query(QueryBuilders.matchQuery("all","如家")).from((page-1)*size).size(size);  //单子段
        handleResponse(request);

    }

    @Test
    void testMultiMatch() throws IOException {
        //1.准备request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备dsl
        request.source().query(QueryBuilders.multiMatchQuery("如家","name","brand")).sort("price", SortOrder.DESC);  //多子段
        handleResponse(request);

    }

    @Test
    void testTerm() throws IOException {
        //1.准备request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备dsl
        request.source().query(QueryBuilders.termQuery("name","如家"));  //精确查询
        handleResponse(request);
    }

    @Test
    void testRange() throws IOException {
        //1.准备request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备dsl
        request.source().query(QueryBuilders.rangeQuery("price").gte(150).lte(200));  //范围查询
        handleResponse(request);
    }

    @Test
    void testBoolQuery() throws IOException {
        //1.准备request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备dsl
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("name","如家"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(150).lte(200));
        request.source().query(boolQueryBuilder);
        handleResponse(request);
    }

    private void handleResponse(SearchRequest request) throws IOException {
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析结果
        SearchHits searchHits = response.getHits();
        //4.1查询条数
        long value = searchHits.getTotalHits().value;
        System.out.println(value);
        //4.2获取数组
        SearchHit[] hits = searchHits.getHits();
        //4.3遍历hits
        for (SearchHit hit : hits) {
            //4.4获取souce
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if(!highlightFields.isEmpty()){
                //根据字段获取高亮结果
                HighlightField highlightField = highlightFields.get("name");
                //获取高亮值
                String name = highlightField.getFragments()[0].toString();
                hotelDoc.setName(name);
            }

            System.out.println(hotelDoc);
        }
    }

    @Test
    void testAggregation() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        //去除文档数据
        request.source().size(0);
        //聚合
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg").field("brand").size(10));
        //发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        Aggregations aggregations = response.getAggregations();
        Terms brandTerms = aggregations.get("brandAgg");
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String brandName = bucket.getKeyAsString();
            System.out.println(brandName);
        }


    }

    @Test
    void testSuggest() throws IOException {
        //准备request
        SearchRequest request = new SearchRequest("hotel");
        //准备DSL
        request.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions", SuggestBuilders.completionSuggestion("suggestion")
                        .prefix("h").skipDuplicates(true).size(10)));
        //发起请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析结果
        Suggest suggest = response.getSuggest();
        //根据补全名称获取补全结果
        CompletionSuggestion suggestion = suggest.getSuggestion("suggestions");
        List<CompletionSuggestion.Entry.Option> options = suggestion.getOptions();
        for (CompletionSuggestion.Entry.Option option : options) {
            String test = option.getText().toString();
            System.out.println(test);
        }
    }

    @BeforeEach
    void setUp() {
        this.client=new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.80.128:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }
}
