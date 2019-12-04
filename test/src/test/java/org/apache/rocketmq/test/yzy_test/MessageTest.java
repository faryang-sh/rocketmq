package org.apache.rocketmq.test.yzy_test;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

/**
 * @author xianming
 * Date    2019-12-04
 */
public class MessageTest {

    @Test
    public void producer() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("p1", true);
        producer.setNamesrvAddr("106.13.140.5:9876");
        producer.start();
        try {
            {
                Message msg = new Message("TopicTest2",
                        "TagA",
                        "OrderID193",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n", sendResult);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
