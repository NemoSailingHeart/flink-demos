package com.demos.datasets;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class DataSetTransformation {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String[] strings = {"1,xiaoming", "2,lihong", "3,zhangsan", "4,lisi"};
        List<String> listStrings = Arrays.asList(strings);
        DataSet<String> data = env.fromCollection(listStrings);

        // map
        mapMethod(data);

        // flatMap
        flatMapMethod(data);

        // mapPartition
        mapPartitionMethod(data);

        // filter
        filterMethod(data);


    }

    private static void filterMethod(DataSet<String> data) throws Exception {
        FilterOperator<String> filter = data.filter(new FilterFunction<String>() {
            public boolean filter(String value) {
                return Integer.valueOf(value.split(",")[0]) > 2;
            }
        });
        System.out.println("======= filter ========");
        filter.print();
    }

    private static void mapPartitionMethod(DataSet<String> data) {
        data.mapPartition(new MapPartitionFunction<String, Long>() {
            public void mapPartition(Iterable<String> values, Collector<Long> out) {
                long c = 0;
                for (String s : values) {
                    c++;
                }
                out.collect(c);
            }
        });
    }

    private static void flatMapMethod(DataSet<String> data) throws Exception {
        FlatMapOperator<String, String> flatMap = data.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String value, Collector<String> out) {
                for (String s : value.split(",")) {
                    out.collect(s);
                }
            }
        });
        System.out.println("========= flatMap ==========");
        flatMap.print();
    }

    private static void mapMethod(DataSet<String> data) throws Exception {
        MapOperator<String, Integer> map = data.map(new MapFunction<String, Integer>() {
            public Integer map(String value) {
                return Integer.parseInt(value.split(",")[0]);
            }
        });
        System.out.println("===== map 执行后的结果 =====");
        map.print();
    }
}
