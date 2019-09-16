package com.demos.wordcount;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author guan
 * @since 2019/2/28
 */
public class WordCountDataTest {
	@Test
	public void getDefaultTextLineDataSet(){
		ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> defaultTextLineDataSet = WordCountData.getDefaultTextLineDataSet(executionEnvironment);
		try {
			long count = defaultTextLineDataSet.count();
			System.out.println(count);
			Assert.assertEquals(35, count);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}