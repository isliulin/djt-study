package com.djt.entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.avatica.util.Quoting.BACK_TICK;

/**
 * SQL组成
 * 可最大化支持Flink SQL原生功能
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-22
 */
@Log4j2
@Data
public class FlinkSqlEntity implements Serializable {

    /**
     * 表名
     */
    @JSONField(name = "table")
    private String table;

    /**
     * 字段
     * e.g. ["f1 as xx1","f2 as xx2","sum(a) as b","count(c) as d"]
     */
    @JSONField(name = "select")
    private String[] select;

    /**
     * 过滤条件
     * e.g. "f1 > 0 AND f2 <= 10"
     */
    @JSONField(name = "where")
    private String where;

    /**
     * 分组字段
     * e.g. ["f1","f2"]
     */
    @JSONField(name = "group_by")
    private String[] groupBy;

    /**
     * 排序
     * e.g. ["f1 ASC", "f2 DESC"]
     */
    @JSONField(name = "order_by")
    private String[] orderBy;

    /**
     * 生成SQL并解析校验
     *
     * @return SQL
     */
    public String buildSql() {
        StringBuilder sql = new StringBuilder();
        if (ArrayUtils.isNotEmpty(select)) {
            sql.append("select\n");
            sql.append(StringUtils.join(select, ",\n"));
            sql.append("\n");
        }
        if (StringUtils.isNotBlank(table)) {
            sql.append("from ").append(table).append("\n");
        }
        if (StringUtils.isNotBlank(where)) {
            sql.append("where ");
            sql.append(where);
            sql.append("\n");
        }
        if (ArrayUtils.isNotEmpty(groupBy)) {
            sql.append("group by ");
            sql.append(StringUtils.join(groupBy, ", "));
            sql.append("\n");
        }
        if (ArrayUtils.isNotEmpty(orderBy)) {
            sql.append("order by ");
            sql.append(StringUtils.join(orderBy, ", "));
            sql.append("\n");
        }
        return parseFlinkSql(sql.toString());
    }

    /**
     * 解析&校验 Flink SQL
     *
     * @param sql sql
     * @return sql
     */
    public static String parseFlinkSql(String sql) {
        Validate.notBlank(sql, "SQL不能为空!");
        List<String> sqlList = new ArrayList<>();
        try {
            SqlParser parser = SqlParser.create(sql, SqlParser.config()
                    .withParserFactory(FlinkSqlParserImpl.FACTORY)
                    .withCaseSensitive(false)
                    .withQuoting(BACK_TICK)
                    .withQuotedCasing(Casing.UNCHANGED)
                    .withUnquotedCasing(Casing.UNCHANGED)
                    .withConformance(FlinkSqlConformance.DEFAULT)
            );
            List<SqlNode> sqlNodeList = parser.parseStmtList().getList();
            if (CollectionUtils.isNotEmpty(sqlNodeList)) {
                for (SqlNode sqlNode : sqlNodeList) {
                    sqlList.add(sqlNode.toString());
                }
            }
        } catch (Exception e) {
            log.error("SQL解析失败:{}", e.getMessage());
            throw new IllegalArgumentException(e);
        }
        return StringUtils.join(sqlList, "\n");
    }

}
