package cn.itcast.hotel.mq;

import cn.itcast.hotel.constants.MqConstants;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author 金宗文
 * @version 1.0
 * 监听队列中的消息
 */
@Component
public class HotelListener {
    @Autowired
    private IHotelService hotelService;



    /**
     * 监听酒店新增或修改的业务
     * @param id 酒店id
     */
    @RabbitListener(queues = MqConstants.HOTEL_INSERT_QUEUE)
    public void listenHotelInsertOrUpdate(Long id){
        System.out.println("监听到酒店新增或修改的消息，酒店id为：" + id);
        // 根据id查询mysql 获取修改或新增的数据信息
        hotelService.insertById(id);

    }

    /**
     * 监听酒店删除的业务
     * @param id 酒店id
     */
    @RabbitListener(queues = MqConstants.HOTEL_DELETE_QUEUE)
    public void listenHotelDelete(Long id){
        System.out.println("监听到酒店删除的消息，酒店id为：" + id);
        // 根据id删除es中的数据
        hotelService.deleteById(id);
    }
}
