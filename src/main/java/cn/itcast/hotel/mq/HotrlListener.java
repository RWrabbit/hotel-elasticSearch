package cn.itcast.hotel.mq;

import cn.itcast.hotel.constants.MqConstants;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HotrlListener {

    @Autowired
    private IHotelService hotelService;

    /**
     * 监听酒店新增或修改
     * @param id 酒店id
     */
    @RabbitListener(queues = MqConstants.HOTEL_INSERT_QUEUE)
    public void listnHotelInsterOrUpdate(Long id){
        hotelService.insertById(id);
    }

    /**
     * 监听酒店新增或修改
     * @param id 酒店id
     */
    @RabbitListener(queues = MqConstants.HOTEL_DELETE_QUEUE)
    public void listnHotelDeleteOrUpdate(Long id){
        hotelService.deleteById(id);
    }
}
