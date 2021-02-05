package org.apache.flink.table.examples.java.basics;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class WindowEmitExample {
	public static void main(String[] args) {
		// {"rowtime":"2021-01-20 00:00:01","msg":"hello"} {"rowtime":"2021-01-20 00:00:02","msg":"hello"}
		// {"rowtime":"2021-01-20 00:00:53","msg":"hello"}
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		String s = "CREATE TABLE test (\n"
			+ "    topic VARCHAR METADATA FROM 'topic',\n"
			+ "    `offset` bigint METADATA,\n"
			+ "    rowtime TIMESTAMP(3),\n"
			+ "    msg VARCHAR,\n"
			+ "    WATERMARK FOR rowtime AS rowtime - INTERVAL '10' SECOND\n"
			+ ") WITH (\n"
			+ "    'connector' = 'kafka', "
			+ "    'topic' = 'test',"
			+ "    'scan.startup.mode' = 'latest-offset',\n"
			+ "    'properties.bootstrap.servers' = 'localhost:9092',\n"
			+ "    'properties.group.id' = 'test',\n"
			+ "    'format' = 'json'\n"
			+ ")";
		String p = "CREATE TABLE printsink (\n"
			+ "    msg VARCHAR,\n"
			+ "    cnt BIGINT"
			+ ") WITH (\n"
			+ "    'connector' = 'print'\n"
			+ ")";
		String sql = "select " +
			"msg," +
			"count(1) cnt" +
			" from test" +
			" where msg = 'hello' " +
			" group by TUMBLE(rowtime, INTERVAL '30' SECOND), msg " +
			" EMIT \n" +
			"  WITH DELAY '1' SECOND BEFORE WATERMARK,\n" +
			"  WITHOUT DELAY AFTER WATERMARK";

		tEnv.executeSql(p);
		tEnv.executeSql(s);

		TableResult re = tEnv.executeSql("insert into printsink " + sql);
		re.print();


	}
}
