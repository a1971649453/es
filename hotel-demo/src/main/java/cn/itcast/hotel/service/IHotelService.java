package cn.itcast.hotel.service;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IHotelService {

    void deleteById(Long id);

    void insertById(Long id);

    Hotel findById(Long id);

    List<Hotel> findAll();
    /**
     * 根据关键字搜索酒店信息
     * @param params 请求参数对象，包含用户输入的关键字
     * @return 酒店文档列表
     */
    PageResult search(RequestParams params) throws IOException;

    Map<String, List<String>> getFilters(RequestParams params);

    List<String> getSuggestions(String prefix);
}
