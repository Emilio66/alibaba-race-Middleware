package com.alibaba.middleware.race.Utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Utils.Constants;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.TableItemFactory;

public class Generator {

    Random rand = new Random();
    byte LINE_END = Constants.LINE_END;

    public void genFile(long lineNum, String filePath, int buyerNum, int productNum) {

        TableItemFactory.initParams(buyerNum, productNum, 0);

        try {
            FileOutputStream fOut = new FileOutputStream(new File(filePath));
            BufferedOutputStream buffOut = new BufferedOutputStream(fOut);

            buffOut.write(String.valueOf(lineNum).getBytes());
            buffOut.write(LINE_END);
            buffOut.write(LINE_END);
            buffOut.write('\n');

            for (int i = 0; i < lineNum; i++) {
                final int platform = rand.nextInt(2);
                final OrderMessage orderMessage = (platform == 0 ? OrderMessage.createTbaoMessage()
                        : OrderMessage.createTmallMessage());
                orderMessage.setCreateTime(System.currentTimeMillis());

                buffOut.write(platform + '0');
                byte[] body = RaceUtils.writeKryoObject(orderMessage);
                buffOut.write(body);
                buffOut.write(LINE_END);
                buffOut.write(LINE_END);
                buffOut.write('\n');

            }

            buffOut.flush();
            buffOut.close();

        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

    }

}
