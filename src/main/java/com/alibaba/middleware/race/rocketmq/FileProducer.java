package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Utils.FileParser;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Semaphore;

public class FileProducer {

    private String filePath;
    private int    count          = 0;

    // name server 的地址, 如127.0.0.1:9876
    private String nameServerAddr = "127.0.0.1:9876";

    // 产生消息分组名称
    private String groupName      = "groupName111";

    public FileProducer(String filePath, String nameServerAddr, String groupName) {
        this.filePath = filePath;
        this.nameServerAddr = nameServerAddr;
        this.groupName = groupName;
    }

    public void start() throws MQClientException, IOException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer(groupName);

        producer.setNamesrvAddr(nameServerAddr);

        producer.start();

        String[] topics = new String[] { RaceConfig.MqTaobaoTradeTopic,
                RaceConfig.MqTmallTradeTopic };
        final Semaphore semaphore = new Semaphore(0);

        FileParser parser = new FileParser(new File(filePath));
        parser.prepare();

        String countStr = new String(parser.nextLine());
        count = Integer.valueOf(countStr);

        for (int i = 0; i < count; i++) {
            try {
                byte[] lineBytes = parser.nextLine();
                if (lineBytes == null) {
                    break;
                }

                int platform = lineBytes[0] == '0' ? 0 : 1;
                byte[] body = Arrays.copyOfRange(lineBytes, 1, lineBytes.length);

                final OrderMessage orderMessage = RaceUtils
                    .readKryoObject(OrderMessage.class, body);
                System.out.println(String.valueOf(i) + orderMessage);

                Message msgToBroker = new Message(topics[platform], body);

                producer.send(msgToBroker, new SendCallback() {
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(orderMessage);
                        semaphore.release();
                    }

                    public void onException(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });

                //Send Pay message
                PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
                double amount = 0;
                for (final PaymentMessage paymentMessage : paymentMessages) {
                    int retVal = Double.compare(paymentMessage.getPayAmount(), 0);
                    if (retVal < 0) {
                        throw new RuntimeException("price < 0 !!!!!!!!");
                    }

                    if (retVal > 0) {
                        amount += paymentMessage.getPayAmount();
                        final Message messageToBroker = new Message(RaceConfig.MqPayTopic,
                            RaceUtils.writeKryoObject(paymentMessage));
                        producer.send(messageToBroker, new SendCallback() {
                            public void onSuccess(SendResult sendResult) {
                                System.out.println(paymentMessage);
                            }

                            public void onException(Throwable throwable) {
                                throwable.printStackTrace();
                            }
                        });
                    } else {
                        //
                    }
                }

                if (Double.compare(amount, orderMessage.getTotalPrice()) != 0) {
                    throw new RuntimeException("totalprice is not equal.");
                }

            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        semaphore.acquire(count);

        //用一个short标识生产者停止生产数据
        byte[] zero = new byte[] { 0, 0 };
        Message endMsgTB = new Message(RaceConfig.MqTaobaoTradeTopic, zero);
        Message endMsgTM = new Message(RaceConfig.MqTmallTradeTopic, zero);
        Message endMsgPay = new Message(RaceConfig.MqPayTopic, zero);

        try {
            producer.send(endMsgTB);
            producer.send(endMsgTM);
            producer.send(endMsgPay);
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.shutdown();

    }

    public static void main(String[] args) throws MQClientException, IOException,
                                          InterruptedException {
        FileProducer producer = new FileProducer("/Users/applex/Documents/data.txt",
            "127.0.0.1:9876", "group");
        producer.start();
    }

    /**
     * Setter method for property <tt>nameServerAddr </tt>.
     *
     * @param nameServerAddr value to be assigned to property nameServerAddr
     */
    public void setNameServerAddr(String nameServerAddr) {
        this.nameServerAddr = nameServerAddr;
    }

    /**
     * Setter method for property <tt>groupName </tt>.
     *
     * @param groupName value to be assigned to property groupName
     */
    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }
}
