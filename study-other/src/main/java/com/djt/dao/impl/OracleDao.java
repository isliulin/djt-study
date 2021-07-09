package com.djt.dao.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.druid.pool.DruidDataSource;
import com.djt.dao.AbstractDao;
import com.djt.utils.DjtConstant;
import com.djt.utils.PasswordUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Oracle DAO
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-01
 */
@Slf4j
public class OracleDao extends AbstractDao {

    /**
     * 单例对象
     */
    private volatile static OracleDao dao;

    private OracleDao(Properties config) {
        super(config);
    }

    public static OracleDao getInstance(Properties config) {
        if (null == dao) {
            synchronized (OracleDao.class) {
                if (null == dao) {
                    dao = new OracleDao(config);
                }
            }
        }
        return dao;
    }

    @Override
    protected void initDataSource(DruidDataSource dataSource, Properties config) {
        try {
            dataSource.setUrl(config.getProperty("oracle.druid.Url"));
            dataSource.setUsername(config.getProperty("oracle.druid.Username"));
            dataSource.setPassword(PasswordUtils.decrypt(config.getProperty("oracle.druid.Password")));
            dataSource.setInitialSize(Integer.parseInt(config.getProperty("oracle.druid.InitialSize", "1")));
            dataSource.setMaxActive(Integer.parseInt(config.getProperty("oracle.druid.MaxActive", "1")));
            dataSource.setMinIdle(Integer.parseInt(config.getProperty("oracle.druid.MinIdle", "1")));
            dataSource.setMaxWait(Long.parseLong(config.getProperty("oracle.druid.MaxWait", "100")));
            dataSource.setKeepAlive(Boolean.parseBoolean(config.getProperty("oracle.druid.KeepAlive", "true")));
            dataSource.setMinEvictableIdleTimeMillis(Long.parseLong(config.getProperty("oracle.druid.MinEvictableIdleTimeMillis", "300000")));
            dataSource.setTimeBetweenEvictionRunsMillis(Long.parseLong(config.getProperty("oracle.druid.TimeBetweenEvictionRunsMillis", "60000")));
            dataSource.setRemoveAbandoned(Boolean.parseBoolean(config.getProperty("oracle.druid.RemoveAbandoned", "false")));
            dataSource.setTestWhileIdle(Boolean.parseBoolean(config.getProperty("oracle.druid.TestWhileIdle", "true")));
            dataSource.setValidationQuery(config.getProperty("oracle.druid.ValidationQuery", "SELECT 1 FROM DUAL"));
            dataSource.setTestOnBorrow(Boolean.parseBoolean(config.getProperty("oracle.druid.TestOnBorrow", "false")));
            dataSource.setTestOnReturn(Boolean.parseBoolean(config.getProperty("oracle.druid.TestOnReturn", "false")));
            dataSource.setPoolPreparedStatements(Boolean.parseBoolean(config.getProperty("oracle.druid.PoolPreparedStatements", "true")));
            dataSource.setMaxPoolPreparedStatementPerConnectionSize(Integer.parseInt(config.getProperty("oracle.druid.MaxPoolPreparedStatementPerConnectionSize", "10")));
            dataSource.setFilters(config.getProperty("oracle.druid.Filters", "stat"));
            dataSource.setConnectionProperties(config.getProperty("oracle.druid.ConnectionProperties"));
        } catch (SQLException e) {
            throw new RuntimeException("数据源初始化失败！", e);
        }
    }

    /**
     * 创建分区
     *
     * @param startDtStr 起始分区(yyyyMMdd)
     * @param endDtStr   截止分区(yyyyMMdd)
     * @param dm         日月标识
     */
    public void createPartition(String tableName, String startDtStr, String endDtStr, String dm) {
        LocalDate startDt = LocalDate.parse(startDtStr, DjtConstant.YMD);
        LocalDate endDt = LocalDate.parse(endDtStr, DjtConstant.YMD);
        DateTimeFormatter dmFormatter = "D".equalsIgnoreCase(dm) ? DjtConstant.YMD : DjtConstant.YM;
        while (!startDt.isAfter(endDt)) {
            String thisDate = startDt.format(dmFormatter);
            String sql = StrUtil.format("ALTER TABLE {} ADD PARTITION P{} VALUES ({})", tableName, thisDate, thisDate);
            try {
                db.execute(sql);
            } catch (SQLException e) {
                log.error("分区 " + thisDate + " 创建失败：" + e.getMessage());
            }
            startDt = "D".equalsIgnoreCase(dm) ? startDt.plusDays(1) : startDt.plusMonths(1);
        }
    }


}
