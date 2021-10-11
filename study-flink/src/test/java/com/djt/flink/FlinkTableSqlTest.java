package com.djt.flink;

import com.djt.entity.FlinkSqlEntity;
import com.djt.event.MyEvent;
import com.djt.utils.EventUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Flink常用Table SQL API测试
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-22
 */
@Log4j2
public class FlinkTableSqlTest extends FlinkBaseTest {

    @Test
    public void testParseExp() {
        List<Expression> expList = ExpressionParser.parseExpressionList("a = 1 and b = 2");
        for (Expression exp : expList) {
            System.out.println(exp.asSummaryString());
        }
    }

    @Test
    public void testTableApi1() throws Exception {
        DataStream<MyEvent> kafkaSource = getKafkaSourceWithWm();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        tableEnv.createTemporaryView("t_test_djt", kafkaSource, EventUtils.getExpressions(MyEvent.class));
        Table table = tableEnv.from("t_test_djt")
                .where($("id").isEqual("666"));
        table.printSchema();
        tableEnv.toAppendStream(table, Row.class).print();
        streamEnv.execute("testTableApi1");
    }

    @Test
    public void testTableApi2() throws Exception {
        DataStream<MyEvent> kafkaSource = getKafkaSourceWithWm();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        Table table = tableEnv.fromDataStream(kafkaSource, EventUtils.getExpressions(MyEvent.class));
        Table windowedTable = table.window(Tumble.over(lit(5).second()).on($("event_time")).as("window"))
                .groupBy($("window"), $("id"))
                .select($("id"),
                        $("num").max().as("num_max"),
                        $("num").sum().as("num_sum"),
                        $("window").start().as("win_start"),
                        $("window").end().as("win_end"));
        //注意:窗口的默认时区为 UTC + 0
        windowedTable.printSchema();
        tableEnv.toRetractStream(windowedTable, Row.class).print();
        streamEnv.execute("testTableApi2");
    }

    @Test
    public void testSqlApi1() throws Exception {
        DataStream<MyEvent> kafkaSource = getKafkaSourceWithWm();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        tableEnv.createTemporaryView("t_test_djt", kafkaSource, EventUtils.getExpressions(MyEvent.class));
        String selectSql = "SELECT id,\n" +
                "       max(num) AS num_max,\n" +
                "       sum(num) AS num_sum\n" +
                "FROM t_test_djt\n" +
                "WHERE id='666'\n" +
                "AND name='张三'\n" +
                "GROUP BY id";
        Table table = tableEnv.sqlQuery(selectSql);
        table.printSchema();
        tableEnv.toRetractStream(table, Row.class).print();
        streamEnv.execute("testSqlApi1");
    }

    @Test
    public void testSqlApi2() throws Exception {
        DataStream<MyEvent> kafkaSource = getKafkaSourceWithWm();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        tableEnv.createTemporaryView("t_test_djt", kafkaSource, EventUtils.getExpressions(MyEvent.class));
        //注意:窗口的默认时区为 UTC + 0
        String selectSql = "SELECT `id`,\n" +
                "       MAX(`num`) AS `num_max`,\n" +
                "       SUM(`num`) AS `num_sum`,\n" +
                "       TUMBLE_START(`event_time`, INTERVAL '5' SECOND) AS `win_start`,\n" +
                "       TUMBLE_END(`event_time`, INTERVAL '5' SECOND) AS `win_end`\n" +
                "FROM t_test_djt\n" +
                "WHERE `name`='张三'\n" +
                "GROUP BY TUMBLE(event_time, INTERVAL '5' SECOND),id";
        String sql = FlinkSqlEntity.parseFlinkSql(selectSql);
        System.out.println(sql);
        Table table = tableEnv.sqlQuery(selectSql);
        table.printSchema();
        tableEnv.toRetractStream(table, Row.class).print();
        streamEnv.execute("testSqlApi2");
    }

    @Test
    public void testSqlApi3() throws Exception {
        DataStream<MyEvent> kafkaSource = getKafkaSourceWithWm();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        //每条数据都触发计算
        tableEnv.getConfig().getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true);
        tableEnv.getConfig().getConfiguration().setInteger("table.exec.emit.early-fire.delay", 0);
        tableEnv.getConfig().getConfiguration().setBoolean("table.exec.emit.late-fire.enabled", true);
        tableEnv.getConfig().getConfiguration().setInteger("table.exec.emit.late-fire.delay", 0);
        tableEnv.getConfig().getConfiguration().setBoolean("table.exec.emit.unchanged.enabled", true);
        //设置状态空闲时间
        //带窗口的SQL 相当于水位没过窗口end_time之后，且再等待该值之后再销毁窗口
        //不带窗口的SQL 相当于状态未被更新的时间(类似系统倒计时)超过该值时，状态销毁重新计算
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        tableEnv.createTemporaryView("t_test_djt", kafkaSource, EventUtils.getExpressions(MyEvent.class));
        //注意:窗口的默认时区为 UTC + 0
        String selectSql = "SELECT `id`,\n" +
                "       MAX(`num`) AS `num_max`,\n" +
                "       SUM(`num`) AS `num_sum`,\n" +
                "       TUMBLE_START(`event_time`, INTERVAL '5' SECOND) AS `win_start`,\n" +
                "       TUMBLE_END(`event_time`, INTERVAL '5' SECOND) AS `win_end`\n" +
                "FROM t_test_djt\n" +
                "WHERE `name`='张三'\n" +
                "GROUP BY TUMBLE(event_time, INTERVAL '5' SECOND),id";
        Table windowTable = tableEnv.sqlQuery(selectSql);
        windowTable.printSchema();
        tableEnv.toRetractStream(windowTable, Row.class).print();
        streamEnv.execute("testSqlApi3");
    }


}
