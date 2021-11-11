package com.dtstack.flink.sql.sink.dorisdb.tools;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ExecuteSQL {
    public static void main(String[] args) throws Exception {
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        if (!params.has("f") || Strings.isNullOrEmpty(params.get("f"))) {
            throw new IllegalArgumentException("No sql file specified.");
        }
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(bsSettings);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] sqls = (String.join("\n", env.readTextFile(params.get("f")).collect()) + "\n").split(";\\s*\n");
        for (String sql : sqls) {
            System.out.println(String.format("Executing SQL: \n%s\n", sql));
            tEnv.executeSql(sql);
        }
    }
}
