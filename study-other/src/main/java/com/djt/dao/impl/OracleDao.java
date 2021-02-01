package com.djt.dao.impl;

import com.djt.dao.AbstractDao;
import com.djt.test.utils.PasswordUtils;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Oracle DAO
 *
 * @author 　djt317@qq.com
 * @date 　  2021-02-01 10:25
 */
public class OracleDao extends AbstractDao {

    /**
     * 单例对象
     */
    private volatile static OracleDao dao;

    private OracleDao(Properties config) {
        super.config = config;
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
    protected void initDataSource() {
        try {
            dataSource.setUrl(config.getProperty("oracle.druid.Url"));
            dataSource.setUsername(config.getProperty("oracle.druid.Username"));
            dataSource.setPassword(PasswordUtils.decrypt(config.getProperty("oracle.druid.Password")));
            dataSource.setInitialSize(Integer.parseInt(config.getProperty("oracle.druid.InitialSize", "3")));
            dataSource.setMaxActive(Integer.parseInt(config.getProperty("oracle.druid.MaxActive", "3")));
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

}
