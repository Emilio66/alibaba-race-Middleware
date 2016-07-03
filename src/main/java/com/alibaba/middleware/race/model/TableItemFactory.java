package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.Utils.Constants;

import java.util.Random;

public class TableItemFactory {

    private static int BUYER_NUMS = Constants.BUYER_NUMS; //买家总数目
    private static int PRODUCT_NUMS = Constants.PRODUCT_NUMS; //商品总数目

    private static int MAX_TOTAL_PRICE = Constants.MAX_TOTAL_PRICE;

    private static long startId = System.currentTimeMillis(); //第一个订单ID
    private static Random rand = new Random();

    private static int randInt(int max) {
        return rand.nextInt(max);
    }

    private static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        long factor = (long) Math.pow(10, places);
        value = value * factor;
        long tmp = Math.round(value);
        return (double) tmp / factor;
    }

    public static String createBuyerId() {
        return "buyer" + randInt(BUYER_NUMS);
    }

    public static long createOrderId() {
        return startId++;
    }

    public static String createProductId() {
        return "product" + randInt(PRODUCT_NUMS);
    }

    public static String createTbaoSalerId() {
        return "tb_saler" + randInt(BUYER_NUMS);
    }
    public static String createTmallSalerId() {
        return "tm_saler" + randInt(BUYER_NUMS);
    }

    public static double createTotalPrice() {
        return round(rand.nextDouble() * MAX_TOTAL_PRICE + 0.1, 2);
    }

    /**
     * 初始化工厂, 请务必在生成一个批次之前调用
     *
     * @param buyerNums      买家数目
     * @param productNums    产品数目
     * @param maxTotalPrice  最大总金额,暂时没有使用
     */
    public static void initParams(int buyerNums, int productNums, int maxTotalPrice) {
        BUYER_NUMS = buyerNums;
        PRODUCT_NUMS = productNums;
        MAX_TOTAL_PRICE = maxTotalPrice;
    }

}
