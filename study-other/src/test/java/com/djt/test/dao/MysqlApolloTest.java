package com.djt.test.dao;

import cn.hutool.db.sql.SqlExecutor;
import com.djt.dao.impl.MySqlDao;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-03-10
 */
@Slf4j
public class MysqlApolloTest extends DaoTest {

    @Override
    protected void initDao() {
        dao = MySqlDao.getInstance(config);
    }

    @Test
    public void testBatchInsert() throws IOException {
        String filePath = "C:\\Users\\duanjiatao\\Desktop\\数据中心\\我的开发\\2021\\迭代3\\apollo配置接入\\";
        String[] fileNameArr = new String[]{
                "xdata-busi-query",
                "xdata-busi-report",
                "xdata-dc-query",
                "xdata-dc-report",
                "xdata-metric",
                "xdata-outer-schedule",
                "xdata-push-server"};

        String clearSql = "delete from test.t_apollo_config";
        String sql = "INSERT INTO test.t_apollo_config (`key`, value, project)\n" +
                "VALUES(?,?,?)";
        try {
            dao.executeSql(clearSql);
            List<Object[]> paramsList = new ArrayList<>();
            for (String fileName : fileNameArr) {
                List<String> lines = FileUtils.readLines(new File(filePath + fileName), "UTF-8");
                for (String line : lines) {
                    if (StringUtils.isBlank(line)) continue;
                    String[] kvArr = StringUtils.split(line, "=");
                    String key = kvArr[0].trim();
                    String value = null;
                    if (kvArr.length == 2) {
                        value = kvArr[1].trim();
                    }

                    Object[] params = new Object[3];
                    params[0] = key;
                    params[1] = value;
                    params[2] = fileName;
                    paramsList.add(params);
                }
            }
            SqlExecutor.executeBatch(conn, sql, paramsList);
            log.info("写入总条数：{}", paramsList.size());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
