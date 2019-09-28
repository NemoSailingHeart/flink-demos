package com.demos.datasets;

import org.apache.flink.api.java.ExecutionEnvironment;

public class SetSerializables {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // avro
        env.getConfig().enableForceAvro();

        // kryo
        env.getConfig().enableForceKryo();

        // customer Serializable class
        // env.getConfig().addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass)
    }
}
