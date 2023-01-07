package cn.itcast.hotel.demo1;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.service.IHotelService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
class HotelDemoApplicationTests {

    @Autowired
    private IHotelService hotelService;

    /**
     * 测试根据id查询酒店信息
     */
    @Test
    public void test01(){
        Hotel hotel = hotelService.findById(36934L);
        System.out.println(hotel);
    }

    /**
     * 查询所有酒店信息
     */
    @Test
    public void test02(){
        List<Hotel> list = hotelService.findAll();
        for (Hotel hotel : list) {
            System.out.println(hotel);
        }
    }
}
