package cn.itcast.hotel.mapper;

import cn.itcast.hotel.pojo.Hotel;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface HotelMapper {

    @Select("select * from tb_hotel where id = #{id}")
    Hotel findById(Long id);

    @Select("select * from tb_hotel ")
    List<Hotel> findAll();
}
