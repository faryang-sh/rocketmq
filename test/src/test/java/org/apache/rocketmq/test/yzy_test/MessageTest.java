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

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.Random;

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
//        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        try {
            {
                for (int i = 0; i < 100000; i++) {
                    Message msg = new Message("topic_3",
                            "TagD",
                            "OrderID4_" + i,
                            ("Hello world 2:" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("%s%n", sendResult);
                    Thread.sleep(5 * 100);
                }
                Thread.sleep(10 * 1000);
                System.out.println("===end===");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void producer2() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("p1", true);
        producer.setNamesrvAddr("106.13.140.5:9876");
//        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        try {
            {
                for (int i = 0; i < 100000; i++) {
                    Message msg = new Message("TopicTest5",
                            "TagA",
                            "OrderID01_" + i,
                            ("Hello world 01:" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("%s%n", sendResult);
                    Thread.sleep(5 * 100);
                }
                Thread.sleep(10 * 1000);
                System.out.println("===end===");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //MessageExt [queueId=0, storeSize=336, queueOffset=1250, sysFlag=0, bornTimestamp=1577344453974, bornHost=/114.84.154.224:7515, storeTimestamp=1577361600937, storeHost=/115.28.212.79:10911, msgId=731CD44F00002A9F000000000AB5D6E4, commitLogOffset=179689188, bodyCRC=1318251557, reconsumeTimes=16, preparedTransactionOffset=0, toString()=Message{topic='topic_2', flag=0, properties={MIN_OFFSET=0, REAL_TOPIC=%RETRY%cg_2, ORIGIN_MESSAGE_ID=731CD44F00002A9F000000000AA10E60, RETRY_TOPIC=topic_2, MAX_OFFSET=1328, KEYS=OrderID2_46, CONSUME_START_TIME=1577363479438, UNIQ_KEY=FE80000000000000AEDE48FFFE001122000018B4AAC28403FD56008A, WAIT=false, DELAY=18, TAGS=TagD, REAL_QID=0}, body=[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 32, 50, 58, 52, 54], transactionId='null'}]

    // ConsumeMessageThread_2 Receive New Messages: [MessageExt [queueId=218, storeSize=214, queueOffset=0, sysFlag=0, bornTimestamp=1575460847105, bornHost=/114.84.154.224:2965, storeTimestamp=1575460847159, storeHost=/115.28.212.79:10911, msgId=731CD44F00002A9F0000000009BC247B, commitLogOffset=163325051, bodyCRC=198614610, reconsumeTimes=0, preparedTransactionOffset=0, toString()=Message{topic='TopicTest2', flag=0, properties={MIN_OFFSET=0, MAX_OFFSET=1, KEYS=OrderID198, CONSUME_START_TIME=1575461776130, UNIQ_KEY=FE80000000000000AEDE48FFFE001122000018B4AAC213BE6DFD0000, WAIT=true, TAGS=TagA}, body=[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100], transactionId='null'}]]
    @Test
    public void conusmer() throws MQClientException, InterruptedException {
        System.out.println("cg_3");
//        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("%DLQ%cg14", true);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg_3", true);
        consumer.setNamesrvAddr("106.13.140.5:9876");
//        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setAdjustThreadPoolNumsThreshold(10L);
        consumer.subscribe("topic_3", "TagB");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeTimestamp("20181109221800");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
//                try {
//                    Thread.sleep(600);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                System.out.println(new Date());
                System.out.println(msgs.get(0));

                try {
                    String msgContent = new String(msgs.get(0).getBody(), RemotingHelper.DEFAULT_CHARSET);
                    System.out.println("msg content:" + msgContent);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                throw new RuntimeException("消费失败");
//                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
        Thread.sleep(90000 * 1000);
    }

    @Test
    public void conusmerDLQ() throws MQClientException, InterruptedException {
        System.out.println("%DLQ%cg_1");
//        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("%DLQ%cg14", true);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("dlq_cg_1", true);
        consumer.setNamesrvAddr("106.13.140.5:9876");
//        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setAdjustThreadPoolNumsThreshold(10L);
        consumer.subscribe("%DLQ%cg_1", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeTimestamp("20181109221800");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try {
                    Thread.sleep(600);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                System.out.println(new Date());
                System.out.println(msgs.get(0));

                try {
                    String msgContent = new String(msgs.get(0).getBody(), RemotingHelper.DEFAULT_CHARSET);
                    System.out.println("msg content:" + msgContent);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                throw new RuntimeException("消费失败");
//                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
        Thread.sleep(90000 * 1000);
    }

    @Test
    public void conusmer02() throws MQClientException, InterruptedException {
        System.out.println("cg14_1");
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg14", true);
//        consumer.setNamesrvAddr("106.13.140.5:9876");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setAdjustThreadPoolNumsThreshold(10L);
        consumer.subscribe("TopicTest5", "TagA");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeTimestamp("20181109221800");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try {
                    Thread.sleep(600);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                System.out.println(new Date());
                System.out.println(msgs.get(0));

                try {
                    String msgContent = new String(msgs.get(0).getBody(), RemotingHelper.DEFAULT_CHARSET);
                    System.out.println("msg content:" + msgContent);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
//                throw new RuntimeException("消费失败");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
        Thread.sleep(90000 * 1000);
    }

    @Test
    public void conusmer12() throws MQClientException, InterruptedException {
        System.out.println("cg14_2");
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg14", true);
        consumer.setNamesrvAddr("106.13.140.5:9876");
        consumer.subscribe("TopicTest4", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeTimestamp("20181109221800");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                System.out.println(new Date());
                System.out.println(msgs.get(0));

                try {
                    String msgContent = new String(msgs.get(0).getBody(), RemotingHelper.DEFAULT_CHARSET);
                    System.out.println("msg content:" + msgContent);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
//                throw new RuntimeException("消费失败");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
        Thread.sleep(90000 * 1000);
    }

    @Test
    public void conusmer2() throws MQClientException, InterruptedException {
        System.out.println("cg15");
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg15", true);
        consumer.setNamesrvAddr("106.13.140.5:9876");
        consumer.subscribe("TopicTest4", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeTimestamp("20181109221800");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                System.out.println(new Date());
                System.out.println(msgs.get(0));

                try {
                    String msgContent = new String(msgs.get(0).getBody(), RemotingHelper.DEFAULT_CHARSET);
                    System.out.println("msg content:" + msgContent);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
//                throw new RuntimeException("消费失败");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
        Thread.sleep(90000 * 1000);
    }

    @Test
    public void traceMsgTest() {
        // split msg content
        "msg body".split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
    }

    @Test
    public void other() {
        Random random = new Random(100);
        for (int i = 0; i < 1000; i++) {
//            int param = Math.abs(random.nextInt() % 99999999) % 1;
//            if (param>0){
//                System.out.println(param);
//            }
            System.out.println(random.nextInt());
        }
    }

    @Test
    public void testPerm() {
        System.out.println(1 << 3);
    }
}
