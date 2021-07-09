package com.djt.dao.impl;

import com.alibaba.druid.pool.DruidDataSource;
import com.djt.dao.AbstractDao;
import com.djt.utils.PasswordUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Hive DAO
 *
 * @author 　djt317@qq.com
 * @since 　 2021-06-28
 */
@Slf4j
public class HiveDao extends AbstractDao {

    /**
     * 单例
     */
    private volatile static HiveDao dao;

    private HiveDao(Properties config) {
        super(config);
    }

    public static HiveDao getInstance(Properties config) {
        if (null == dao) {
            synchronized (HiveDao.class) {
                if (null == dao) {
                    dao = new HiveDao(config);
                }
            }
        }
        return dao;
    }

    @Override
    protected void initDataSource(DruidDataSource dataSource, Properties config) {
        try {
            dataSource.setUrl(config.getProperty("hive.druid.Url"));
            dataSource.setUsername(config.getProperty("hive.druid.Username"));
            dataSource.setPassword(PasswordUtils.decrypt(config.getProperty("hive.druid.Password")));
            dataSource.setInitialSize(Integer.parseInt(config.getProperty("hive.druid.InitialSize", "3")));
            dataSource.setMaxActive(Integer.parseInt(config.getProperty("hive.druid.MaxActive", "3")));
            dataSource.setMinIdle(Integer.parseInt(config.getProperty("hive.druid.MinIdle", "1")));
            dataSource.setMaxWait(Long.parseLong(config.getProperty("hive.druid.MaxWait", "100")));
            dataSource.setKeepAlive(Boolean.parseBoolean(config.getProperty("hive.druid.KeepAlive", "true")));
            dataSource.setMinEvictableIdleTimeMillis(Long.parseLong(config.getProperty("hive.druid.MinEvictableIdleTimeMillis", "300000")));
            dataSource.setTimeBetweenEvictionRunsMillis(Long.parseLong(config.getProperty("hive.druid.TimeBetweenEvictionRunsMillis", "60000")));
            dataSource.setRemoveAbandoned(Boolean.parseBoolean(config.getProperty("hive.druid.RemoveAbandoned", "false")));
            dataSource.setTestWhileIdle(Boolean.parseBoolean(config.getProperty("hive.druid.TestWhileIdle", "true")));
            dataSource.setValidationQuery(config.getProperty("hive.druid.ValidationQuery", "SELECT 1 FROM DUAL"));
            dataSource.setTestOnBorrow(Boolean.parseBoolean(config.getProperty("hive.druid.TestOnBorrow", "false")));
            dataSource.setTestOnReturn(Boolean.parseBoolean(config.getProperty("hive.druid.TestOnReturn", "false")));
            dataSource.setPoolPreparedStatements(Boolean.parseBoolean(config.getProperty("hive.druid.PoolPreparedStatements", "true")));
            dataSource.setMaxPoolPreparedStatementPerConnectionSize(Integer.parseInt(config.getProperty("hive.druid.MaxPoolPreparedStatementPerConnectionSize", "10")));
            dataSource.setFilters(config.getProperty("hive.druid.Filters", "stat"));
            dataSource.setConnectionProperties(config.getProperty("hive.druid.ConnectionProperties"));
        } catch (SQLException e) {
            throw new RuntimeException("数据源初始化失败！", e);
        }
    }

}
