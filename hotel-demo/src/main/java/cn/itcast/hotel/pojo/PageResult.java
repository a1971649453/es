package cn.itcast.hotel.pojo;

import lombok.Data;

import java.util.List;

/**
 * @author 金宗文
 * @version 1.0
 */
@Data
public class PageResult {
    private Long total;
    private List<HotelDoc> hotels;

    public PageResult() {
    }

    public PageResult(Long total, List<HotelDoc> hotels) {
        this.total = total;
        this.hotels = hotels;
    }
}