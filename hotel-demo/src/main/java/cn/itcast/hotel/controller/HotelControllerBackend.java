//package cn.itcast.hotel.controller;
//
//import cn.itcast.hotel.pojo.PageResult;
//import cn.itcast.hotel.pojo.RequestParams;
//import cn.itcast.hotel.service.IHotelService;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.web.bind.annotation.PostMapping;
//import org.springframework.web.bind.annotation.RequestBody;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RestController;
//
//import java.io.IOException;
//
///**
// * @author 金宗文
// * @version 1.0
// */
//@RestController
//@RequestMapping("/hotel")
//public class HotelControllerBackend {
//
//    @Autowired
//    private IHotelService hotelService;
//    //搜索酒店数据
//    @PostMapping("/list")
//    public PageResult search(@RequestBody RequestParams params) throws IOException {
//        return hotelService.search(params);
//    }
//}