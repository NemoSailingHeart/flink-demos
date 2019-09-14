package com.demos.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * •获得一个execution environment，
 * •加载/创建初始数据，
 * •指定此数据的转换，
 * •指定放置计算结果的位置，
 * •触发程序执行
 */
public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }
        String hostName = args[0];
        Integer port = Integer.parseInt(args[1]);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream(hostName, port);

        DataStream<Tuple2<String, Integer>> counts =
        text.flatMap(new FlinkWordCount.LineSplitter())
                .keyBy(0)
                .sum(1);
        counts.print();
        env.execute("Java WordCount from SocketTextStream Example");

    }
}
