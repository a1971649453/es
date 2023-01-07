package cn.itcast.hotel.pojo;

import lombok.Data;

/**
 * @author 金宗文
 * @version 1.0
 */
@Data
public class RequestParams {
    //搜索内容
    private String key;
    //起始页码
    private Integer page;
    //每页大小
    private Integer size;
    //排序字段
    private String sortBy;

    private String city;
    private String brand;
    private String starName;
    private Integer minPrice;
    private Integer maxPrice;
    // 我当前的地理坐标
    private String location;
}