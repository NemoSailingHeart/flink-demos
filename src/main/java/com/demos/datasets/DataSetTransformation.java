package com.demos.datasets;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.api.java.aggregation.Aggregations.MIN;
import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

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

        // reduce
        reduceMethod(data);

        // reduceGroup
        reduceGroupMethod(data);

        // aggregate
        aggregateMethod(data);

        // distinct
        data.distinct();

        // join
        joinMethod(data);

        // outer join
        outerJoinMethod(data);

        // coGroup
        coGroupMethod(data);

        // cross join
        crossJoinMethod(data);

        // Union
        // Rebalance
    }

    private static void crossJoinMethod(DataSet<String> data) throws Exception {
        DataSet<Integer> data1 = data.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.valueOf(s.split(",")[0]);
            }
        });
        DataSet<String> data2 = data;
        DataSet<Tuple2<Integer, String>> result = data1.cross(data2);
        result.print();
    }

    /**
     * reduce 算子操作的二维变体。将一个或多个字段上的每个输入分组，然后关联组。
     * 每对组调用转换函数。
     */
    private static void coGroupMethod(DataSet<String> data) throws Exception {
        MapOperator<String, String> map = data.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        });

        CoGroupOperator<String, String, String> coGroup = data.coGroup(map)
                .where(0)
                .equalTo(1)
                .with(new CoGroupFunction<String, String, String>() {
                    public void coGroup(Iterable<String> in1, Iterable<String> in2, Collector<String> out) {
                        out.collect(in1.iterator().next());
                    }
                });

        coGroup.print();
    }


    private static void outerJoinMethod(DataSet<String> data) throws Exception {
        MapOperator<String, String> map = data.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) {
                return s.split(",")[2];
            }
        });
        JoinOperator<String, String, String> with = data.leftOuterJoin(map) // rightOuterJoin or fullOuterJoin for right or full outer joins
                .where(0)              // key of the first input (tuple field 0)
                .equalTo(1)            // key of the second input (tuple field 1)
                .with(new JoinFunction<String, String, String>() {
                    public String join(String v1, String v2) {
                        // NOTE:
                        // - v2 might be null for leftOuterJoin
                        // - v1 might be null for rightOuterJoin
                        return v1;
                    }
                });
        with.print();
    }

    private static void joinMethod(DataSet<String> data) throws Exception {
        MapOperator<String, String> map = data.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) {
                return s.split(",")[2];
            }
        });
        JoinOperator.DefaultJoin<String, String> stringStringDefaultJoin = map.join(data)
                .where(1)
                .equalTo(1);
        stringStringDefaultJoin.print();
    }

    private static void aggregateMethod(DataSet<String> data) throws Exception {
        // aggregate 将一组值聚合为单个值。聚合函数可以被认为是内置的reduce函数。
        // 聚合可以应用于完整数据集或分组数据集。
        MapOperator<String, Tuple3<Integer, String, Double>> input = data.map(new MapFunction<String, Tuple3<Integer, String, Double>>() {
            @Override
            public Tuple3<Integer, String, Double> map(String s) {
                String[] split = s.split(",");
                Tuple3<Integer, String, Double> tuple3 = new Tuple3<>();
                tuple3.setFields(Integer.valueOf(split[0]), s, Double.valueOf(split[0]));
                return tuple3;
            }
        });
        DataSet<Tuple3<Integer, String, Double>> output = input.aggregate(SUM, 0).and(MIN, 2);
        //您还可以使用简写语法进行最小，最大和总和聚合。
        DataSet<Tuple3<Integer, String, Double>> output1 = input.sum(0).andMin(2);

        output.print();
        output1.print();

    }

    private static void reduceGroupMethod(DataSet<String> data) throws Exception {
        // 将一组数据元组合成一个或多个数据元。ReduceGroup可以应用于完整数据集或分组数据集。
        GroupReduceOperator<String, Integer> reduceGroup = data.reduceGroup(new GroupReduceFunction<String, Integer>() {
            @Override
            public void reduce(Iterable<String> strings, Collector<Integer> out) {
                int prefixSum = 0;
                for (String str : strings) {
                    prefixSum += Integer.valueOf(str.split(",")[0]);
                    out.collect(prefixSum);
                }

            }
        });

        reduceGroup.print();
    }

    private static void reduceMethod(DataSet<String> data) throws Exception {
        // 先通过map截取数据中的int值
        MapOperator<String, Integer> map = data.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) {
                return Integer.valueOf(s.split(",")[0]);
            }
        });
        // 再进行reduce计算
        ReduceOperator<Integer> reduce = map.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer a, Integer b) {
                return a + b;
            }
        });

        reduce.print();
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
                for (String ignored : values) {
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
