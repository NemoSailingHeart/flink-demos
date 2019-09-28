package com.demos.datasets;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Arrays;

public class DataSetSinks implements Serializable {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] strings = {"1,xiaoming", "2,james", "3,zhangsan", "4,lisi"};

        // text data
        DataSource<String> textData  = env.fromCollection(Arrays.asList(strings));

        // write DataSet to a file on the local file system
        textData.writeAsText("file:///my/result/on/localFS");

        // write DataSet to a file on a HDFS with a namenode running at nnHost:nnPort
        textData.writeAsText("hdfs://nnHost:nnPort/my/result/on/localFS");

        // write DataSet to a file and overwrite the file if it exists
        textData.writeAsText("file:///my/result/on/localFS", FileSystem.WriteMode.OVERWRITE);

        // tuples as lines with pipe as the separator "a|b|c"
        DataSet<Tuple3<String, Integer, Double>> values = textData.map(new MapFunction<String, Tuple3<String, Integer, Double>>() {
            @Override
            public Tuple3<String, Integer, Double> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple3<>(split[1], Integer.valueOf(split[0]), Double.valueOf(split[1])
                );
            }
        });
        values.writeAsCsv("file:///path/to/the/result/file", "\n", "|");

        // this writes tuples in the text formatting "(a, b, c)", rather than as CSV lines
        values.writeAsText("file:///path/to/the/result/file");

        // this writes values as strings using a user-defined TextFormatter object
        values.writeAsFormattedText("file:///path/to/the/result/file",
                new TextOutputFormat.TextFormatter<Tuple3<String, Integer, Double>>() {
                    @Override
                    public String format(Tuple3<String, Integer, Double> value) {
                        return value.f0 + "-" +value.f1 + "-" +value.f2;
                    }
                });

        // write Tuple DataSet to a relational database


        values.map(new MapFunction<Tuple3<String, Integer, Double>, Row>() {
            @Override
            public Row map(Tuple3<String, Integer, Double> tuple3) throws Exception {
                Row row = new Row(3);
                row.setField(1,tuple3.f0);
                row.setField(2,tuple3.f1);
                row.setField(3,tuple3.f2);
                return row;
            }
        }).output(
                // build and configure OutputFormat
                JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                        .setDBUrl("jdbc:derby:memory:persons")
                        .setQuery("insert into persons (name, age, height) values (?,?,?)")
                        .finish()
        );
    }
}
