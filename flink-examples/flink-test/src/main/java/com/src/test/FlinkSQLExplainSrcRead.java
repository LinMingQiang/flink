package com.src.test;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.src.scala.manager.DDLSourceSQLManager;

/** sss. */
public class FlinkSQLExplainSrcRead {

    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings sett = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, sett);
        tableEnv.getConfig()
                .getConfiguration()
                .set(
                        ConfigOptions.key("table.dynamic-table-options.enabled")
                                .booleanType()
                                .defaultValue(true),
                        true);
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka(
                        "localhost:9092", "test", "test", "test", "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createHbaseLookupSource("hbaselookup"));
        String selectSQL =
                "select * from test t1  LEFT JOIN hbaselookup /*+ OPTIONS('lookup.join.pre-partition'='true') */ FOR SYSTEM_TIME AS OF t1.proctime as t2 ON t1.msg = t2.word";
        System.out.println(tableEnv.explainSql(selectSQL));
    }
}
