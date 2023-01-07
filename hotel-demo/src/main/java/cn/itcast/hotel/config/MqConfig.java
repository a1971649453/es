package cn.itcast.hotel.config;

import cn.itcast.hotel.constants.MqConstants;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author 金宗文
 * @version 1.0
 */
@Configuration
public class MqConfig {
    /**
     * 创建交换机
     * @return
     */
    @Bean
    public TopicExchange topicExchange(){
        return new TopicExchange(MqConstants.HOTEL_EXCHANGE, true, false);
    }

    /**
     * 创建队列 存放新增和修改的消息
     * @return
     */
    @Bean
    public Queue insertQueue(){
        return new Queue(MqConstants.HOTEL_INSERT_QUEUE, true);
    }

    /**
     * 创建队列 存放删除的消息
     * @return
     */
    @Bean
    public Queue deleteQueue(){
        return new Queue(MqConstants.HOTEL_DELETE_QUEUE, true);
    }

    /**
     * 绑定交换机和队列
     * 将队列绑定到交换机上，并且指定路由键 insert
     * @return
     */
    @Bean
    public Binding insertQueueBinding(){
        return BindingBuilder.bind(insertQueue()).to(topicExchange()).with(MqConstants.HOTEL_INSERT_KEY);
    }
    /**
     * 绑定交换机和队列
     * 将队列绑定到交换机上，并且指定路由键 delete
     * @return
     */
    @Bean
    public Binding deleteQueueBinding(){

        return BindingBuilder.bind(deleteQueue()).to(topicExchange()).with(MqConstants.HOTEL_DELETE_KEY);
    }
}