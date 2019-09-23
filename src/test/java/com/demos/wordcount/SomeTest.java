package com.demos.wordcount;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class SomeTest {
    @Test
    public void test1() throws IOException {
        File file = new File("C:\\Users\\Guan\\IdeaProjects\\flink-demos\\src\\main\\resources\\kj01\\filedir\\file1.xml");
        if (file.exists()){
            file.delete();
        }
        new File(file.getParent()).mkdirs();
        file.createNewFile();
    }
}
