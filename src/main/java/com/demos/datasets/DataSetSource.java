package com.demos.datasets;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.flink.types.Row;
import org.apache.hadoop.io.IntWritable;

import javax.xml.soap.Text;
import java.io.IOException;

@SuppressWarnings("unused")
public class DataSetSource {
    public static void main(String[] args) throws IOException {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从本地文件系统读
        DataSet<String> localLines = env.readTextFile("file:///path/to/my/textfile");

        // 读取HDFS文件
        DataSet<String> hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile");

        // 读取CSV文件
        DataSet<Tuple3<Integer, String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file").types(Integer.class, String.class, Double.class);

        // 读取CSV文件中的部分
        DataSet<Tuple2<String, Double>> csvInput1 = env.readCsvFile("hdfs:///the/CSV/file").includeFields("10010").types(String.class, Double.class);

        // 读取CSV映射为一个java类
        DataSet<Person> csvInput2 = env.readCsvFile("hdfs:///the/CSV/file").pojoType(Person.class, "name", "age", "zipcode");

        // 读取一个指定位置序列化好的文件
        DataSet<Tuple2<IntWritable, Text>> tuples =
                env.createInput(HadoopInputs.readSequenceFile(IntWritable.class, Text.class, "hdfs://nnHost:nnPort/path/to/file"));

        // 从输入字符创建
        DataSet<String> value = env.fromElements("Foo", "bar", "foobar", "fubar");

        // 创建一个数字序列
        DataSet<Long> numbers = env.generateSequence(1, 10000000);

        // 从关系型数据库读取
        DataSet<Row> dbData = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                .setDBUrl("jdbc:derby:memory:persons")
                .setQuery("select name, age from persons")
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
                .finish()
        );
    }
}
