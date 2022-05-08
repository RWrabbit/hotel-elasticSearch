package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

public class HotelIndexTest {
    private RestHighLevelClient client;



    @Test
    void createHotelIndex() throws IOException {
        //1.创建requset对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        //2. 准备请求的参数：dsl语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        //3.发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void deleteHotelIndex() throws IOException {
        //1.创建requset对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        //2.发送请求
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void existHotelIndex() throws IOException {
        //1.创建requset对象
        GetIndexRequest request = new GetIndexRequest("hotel");
        //2.发送请求
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists?"存在":"不存在");
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
