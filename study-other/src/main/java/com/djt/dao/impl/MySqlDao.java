package com.djt.dao.impl;

import com.djt.dao.AbstractDao;
import com.djt.utils.PasswordUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Properties;

/**
 * MySql DAO
 *
 * @author 　djt317@qq.com
 * @date 　  2021-01-29 17:27
 */
@Slf4j
public class MySqlDao extends AbstractDao {

    /**
     * 单例
     */
    private volatile static MySqlDao dao;

    private MySqlDao(Properties config) {
        super(config);
    }

    public static MySqlDao getInstance(Properties config) {
        if (null == dao) {
            synchronized (MySqlDao.class) {
                if (null == dao) {
                    dao = new MySqlDao(config);
                }
            }
        }
        return dao;
    }

    @Override
    protected void initDataSource() {
        try {
            dataSource.setUrl(config.getProperty("mysql.druid.Url"));
            dataSource.setUsername(config.getProperty("mysql.druid.Username"));
            dataSource.setPassword(PasswordUtils.decrypt(config.getProperty("mysql.druid.Password")));
            dataSource.setInitialSize(Integer.parseInt(config.getProperty("mysql.druid.InitialSize", "3")));
            dataSource.setMaxActive(Integer.parseInt(config.getProperty("mysql.druid.MaxActive", "3")));
            dataSource.setMinIdle(Integer.parseInt(config.getProperty("mysql.druid.MinIdle", "1")));
            dataSource.setMaxWait(Long.parseLong(config.getProperty("mysql.druid.MaxWait", "100")));
            dataSource.setKeepAlive(Boolean.parseBoolean(config.getProperty("mysql.druid.KeepAlive", "true")));
            dataSource.setMinEvictableIdleTimeMillis(Long.parseLong(config.getProperty("mysql.druid.MinEvictableIdleTimeMillis", "300000")));
            dataSource.setTimeBetweenEvictionRunsMillis(Long.parseLong(config.getProperty("mysql.druid.TimeBetweenEvictionRunsMillis", "60000")));
            dataSource.setRemoveAbandoned(Boolean.parseBoolean(config.getProperty("mysql.druid.RemoveAbandoned", "false")));
            dataSource.setTestWhileIdle(Boolean.parseBoolean(config.getProperty("mysql.druid.TestWhileIdle", "true")));
            dataSource.setValidationQuery(config.getProperty("mysql.druid.ValidationQuery", "SELECT 1 FROM DUAL"));
            dataSource.setTestOnBorrow(Boolean.parseBoolean(config.getProperty("mysql.druid.TestOnBorrow", "false")));
            dataSource.setTestOnReturn(Boolean.parseBoolean(config.getProperty("mysql.druid.TestOnReturn", "false")));
            dataSource.setPoolPreparedStatements(Boolean.parseBoolean(config.getProperty("mysql.druid.PoolPreparedStatements", "true")));
            dataSource.setMaxPoolPreparedStatementPerConnectionSize(Integer.parseInt(config.getProperty("mysql.druid.MaxPoolPreparedStatementPerConnectionSize", "10")));
            dataSource.setFilters(config.getProperty("mysql.druid.Filters", "stat"));
            dataSource.setConnectionProperties(config.getProperty("mysql.druid.ConnectionProperties"));
        } catch (SQLException e) {
            throw new RuntimeException("数据源初始化失败！", e);
        }
    }

}
