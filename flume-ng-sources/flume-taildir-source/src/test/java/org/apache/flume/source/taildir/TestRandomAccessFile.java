package org.apache.flume.source.taildir;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;

public class TestRandomAccessFile {

    @Test
    public void test() throws IOException, InterruptedException {
        RandomAccessFile r = new RandomAccessFile("/Users/wgcn/testFile", "r");
        HashSet<String> strings = new HashSet<String>() {
            {
                add("");
                add("");
                add("");
            }
        };

        int size = strings.size();

        while (true){
            long filePointer = r.getFilePointer();
            long length = r.length();
            Thread.sleep(1000);
        }
    }


}
