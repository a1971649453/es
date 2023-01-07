package cn.itcast.hotel.controller;

import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author 金宗文
 * @version 1.0
 */
@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Autowired
    private IHotelService hotelService;
    //搜索酒店数据
    @PostMapping("/list")
    public PageResult search(@RequestBody RequestParams params) throws IOException {
        return hotelService.search(params);
    }

    /**
     * 根据条件对品牌 城市 星级聚合,展示满足条件的数据信息
     * @param params
     * @return
     */
    @PostMapping("filters")
    public Map<String, List<String>> getFilters(@RequestBody RequestParams params){
        return hotelService.getFilters(params);
    }

    /**
     * 自动补全
     * @param prefix
     * @return
     */
    @GetMapping("suggestion")
    public List<String> getSuggestions(@RequestParam("key") String prefix) {
        return hotelService.getSuggestions(prefix);
    }
}