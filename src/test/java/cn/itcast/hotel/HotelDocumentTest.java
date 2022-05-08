package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelDocumentTest {
    private RestHighLevelClient client;
    @Autowired
    private IHotelService hotelService;

    @Test
    void indexAddDocument() throws IOException {
        //根据id查询酒店数据
        Hotel hotel = hotelService.getById(36934);
        //转换文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);
        //1.准备request对象
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        //2.准备json文档
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        //3.发送请求
        client.index(request,RequestOptions.DEFAULT);
    }

    @Test
    void testGetDocById() throws IOException {
        //1.准备Request
        GetRequest request = new GetRequest("hotel", "36934");
        //2.发送请求，得到响应
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //3.解析响应结果
        String json = response.getSourceAsString();
        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
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
