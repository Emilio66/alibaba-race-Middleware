package com.alibaba.middleware.race.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class FileParser {
    private static final byte LINE_END    = Constants.LINE_END;
    MappedByteBuffer          byteBuffer  = null;
    FileChannel               fileChannel = null;

    private final File        file;

    public FileParser(final File file) {
        this.file = file;
    }

    public boolean prepare() {
        try {
            final FileChannel fileChannel = new FileInputStream(this.file).getChannel();
            byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length());
            byteBuffer.load();

            byteBuffer.position(0);

            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public byte[] nextLine() {
        byte[] line = null;
        if (byteBuffer.hasRemaining()) {
            line = readLine(byteBuffer);
        }
        return line;
    }

    public boolean close() {
        try {
            fileChannel.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private byte[] readLine(ByteBuffer byteBuffer) {

        int id = 0;
        byte b;
        byte[] line = new byte[200];
        int i = 0;

        try {
            b = byteBuffer.get();
            int hit = 0;

            while (hit < 2) {
                line[i] = b;
                i++;
                b = byteBuffer.get();

                if (b != LINE_END) {
                    if (hit > 0) {
                        hit = 0;
                    }
                } else {
                    hit++;
                }
            }

            byteBuffer.get();
        } catch (BufferUnderflowException e) {
            if (i == 0) {
                return null;
            }
        }

        return Arrays.copyOfRange(line, 0, i - 1);
    }

}
