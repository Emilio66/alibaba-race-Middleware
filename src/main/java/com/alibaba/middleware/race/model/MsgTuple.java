package com.alibaba.middleware.race.model;


import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.TupleExt;

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

public class MsgTuple implements Serializable, TupleExt{

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

    @Override
    public GlobalStreamId getSourceGlobalStreamid() {
        return null;
    }

    @Override
    public String getSourceComponent() {
        return null;
    }

    @Override
    public int getSourceTask() {
        return 0;
    }

    @Override
    public String getSourceStreamId() {
        return null;
    }

    @Override
    public MessageId getMessageId() {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean contains(String s) {
        return false;
    }

    @Override
    public Fields getFields() {
        return null;
    }

    @Override
    public int fieldIndex(String s) {
        return 0;
    }

    @Override
    public List<Object> select(Fields fields) {
        return null;
    }

    @Override
    public Object getValue(int i) {
        return null;
    }

    @Override
    public String getString(int i) {
        return null;
    }

    @Override
    public Integer getInteger(int i) {
        return null;
    }

    @Override
    public Long getLong(int i) {
        return null;
    }

    @Override
    public Boolean getBoolean(int i) {
        return null;
    }

    @Override
    public Short getShort(int i) {
        return null;
    }

    @Override
    public Byte getByte(int i) {
        return null;
    }

    @Override
    public Double getDouble(int i) {
        return null;
    }

    @Override
    public Float getFloat(int i) {
        return null;
    }

    @Override
    public byte[] getBinary(int i) {
        return new byte[0];
    }

    @Override
    public Object getValueByField(String s) {
        return null;
    }

    @Override
    public String getStringByField(String s) {
        return null;
    }

    @Override
    public Integer getIntegerByField(String s) {
        return null;
    }

    @Override
    public Long getLongByField(String s) {
        return null;
    }

    @Override
    public Boolean getBooleanByField(String s) {
        return null;
    }

    @Override
    public Short getShortByField(String s) {
        return null;
    }

    @Override
    public Byte getByteByField(String s) {
        return null;
    }

    @Override
    public Double getDoubleByField(String s) {
        return null;
    }

    @Override
    public Float getFloatByField(String s) {
        return null;
    }

    @Override
    public byte[] getBinaryByField(String s) {
        return new byte[0];
    }

    @Override
    public List<Object> getValues() {
        return null;
    }

    @Override
    public int getTargetTaskId() {
        return 0;
    }

    @Override
    public void setTargetTaskId(int i) {

    }

    @Override
    public long getCreationTimeStamp() {
        return 0;
    }

    @Override
    public void setCreationTimeStamp(long l) {

    }

}
