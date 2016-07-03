package com.alibaba.middleware.race.model;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaoz on 2016/7/1.
 */
public class MsgTuple implements Serializable{
    private static final long serialVersionUID= 783249752131L;

    protected final List<MessageExt> msgList;
    protected final MessageQueue msgQueue;

    //统计消息失败次数
    protected final AtomicInteger failureTimes;
    protected final long createTime; //创建时间
    protected long emitTime; //重新发送时间

    protected transient CountDownLatch latch;
    protected transient boolean isSuccess;

    public MsgTuple(List<MessageExt> list, MessageQueue mq){
        this.msgList = list;
        this.msgQueue = mq;

        this.failureTimes = new AtomicInteger(0);
        this.createTime = System.currentTimeMillis();

        this.latch = new CountDownLatch(1);
        this.isSuccess = false;
    }

    public AtomicInteger getFailureTimes() {
        return failureTimes;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getEmitTime() {
        return emitTime;
    }

    public List<MessageExt> getMsgList() {
        return msgList;
    }


    public MessageQueue getMq() {
        return msgQueue;
    }

    //跟新发送时间
    public void updateEmitTime(){
        this.emitTime = System.currentTimeMillis();
    }

    //等待完成
    public boolean waitFinish() throws InterruptedException {
        return latch.await(4, TimeUnit.HOURS);
    }

    public void done() {
        isSuccess = true;
        latch.countDown();
    }

    public void fail() {
        isSuccess = false;
        latch.countDown();
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
