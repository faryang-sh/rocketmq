package org.apache.rocketmq.test.yzy_test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.trace.TraceConstants;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;

import java.util.List;

/**
 * @author xianming
 * Date    2019-12-04
 */
public class MessageTest {

    // SendResult [sendStatus=SEND_OK, msgId=FE80000000000000AEDE48FFFE001122000018B4AAC213D7B2310000, offsetMsgId=731CD44F00002A9F0000000009BC2D7D, messageQueue=MessageQueue [topic=TopicTest2, brokerName=broker-a, queueId=150], queueOffset=0]
    @Test
    public void producer() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("p1", true);
        producer.setNamesrvAddr("106.13.140.5:9876");
        producer.start();
        try {
            {
                Message msg = new Message("TopicTest2",
                        "TagA",
                        "OrderID200",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n", sendResult);
                Thread.sleep(10 * 1000);
                System.out.println("===end===");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ConsumeMessageThread_2 Receive New Messages: [MessageExt [queueId=218, storeSize=214, queueOffset=0, sysFlag=0, bornTimestamp=1575460847105, bornHost=/114.84.154.224:2965, storeTimestamp=1575460847159, storeHost=/115.28.212.79:10911, msgId=731CD44F00002A9F0000000009BC247B, commitLogOffset=163325051, bodyCRC=198614610, reconsumeTimes=0, preparedTransactionOffset=0, toString()=Message{topic='TopicTest2', flag=0, properties={MIN_OFFSET=0, MAX_OFFSET=1, KEYS=OrderID198, CONSUME_START_TIME=1575461776130, UNIQ_KEY=FE80000000000000AEDE48FFFE001122000018B4AAC213BE6DFD0000, WAIT=true, TAGS=TagA}, body=[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100], transactionId='null'}]]
    @Test
    public void conusmer() throws MQClientException, InterruptedException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg1", true);
        consumer.setNamesrvAddr("106.13.140.5:9876");
        consumer.subscribe("TopicTest2", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeTimestamp("20181109221800");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                throw new RuntimeException("消费失败");
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
        Thread.sleep(900 * 1000);
    }

    @Test
    public void traceMsgTest() {
        // split msg content
        "msg body".split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
    }
}
