package com.nemo.DemoTest;

import org.junit.Test;

/**
 * Created by Nemo on 2018/3/11.
 */
public class HelloTest {
    @Test
    public void test_exception(){
        try {
            int i = 0;
            int s = 3;
            System.out.println( s  / i);
        } finally {
            System.out.println("end");
        }


    }

}
