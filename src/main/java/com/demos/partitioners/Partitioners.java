package com.demos.partitioners;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * 介绍几种分区器
 */
public class Partitioners {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String[] strings = {"1,xiaoming", "2,james", "3,zhangsan", "4,lisi"};
        List<String> listStrings = Arrays.asList(strings);
        DataSet<String> data = env.fromCollection(listStrings);

        MapOperator<String, String> map = data.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s.split(",")[1];
            }
        });

        // hash partitioner
        DataSet<Tuple2<String,Integer>> in = data.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<String,Integer>(split[1],Integer.valueOf(split[0]));
            }
        });

        DataSet<Integer> result = in.partitionByHash(0)
                        .mapPartition(new MapPartitionFunction<Tuple2<String, Integer>, Integer>() {
                            @Override
                            public void mapPartition(Iterable<Tuple2<String, Integer>> iterable, Collector<Integer> collector) throws Exception {
                                for (Tuple2<String, Integer> stringIntegerTuple2 : iterable) {
                                    System.out.println(stringIntegerTuple2);
                                }
                            }
                        });

        // range partitioner
        DataSet<Integer> result1 = in.partitionByRange(0)
                .mapPartition(new MapPartitionFunction<Tuple2<String, Integer>, Integer>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<String, Integer>> iterable, Collector<Integer> collector) throws Exception {
                        for (Tuple2<String, Integer> stringIntegerTuple2 : iterable) {
                            System.out.println(stringIntegerTuple2);
                        }
                    }
                });

        // customer partitioner
        DataSet<Integer> result2 = in.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String s, int i) {
                return 0;
            }
        }, 1).mapPartition(new MapPartitionFunction<Tuple2<String, Integer>, Integer>() {
            @Override
            public void mapPartition(Iterable<Tuple2<String, Integer>> iterable, Collector<Integer> collector) throws Exception {
                for (Tuple2<String, Integer> stringIntegerTuple2 : iterable) {
                    System.out.println(stringIntegerTuple2);
                }
            }
        });

        // Sort Partition
        in.sortPartition(1, Order.ASCENDING)
                .mapPartition(new MapPartitionFunction<Tuple2<String, Integer>, Object>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {
                        for (Tuple2<String, Integer> stringIntegerTuple2 : iterable) {
                            System.out.println(stringIntegerTuple2);
                        }
                    }
                });

        // first n
        in.first(1).print();

    }
}
