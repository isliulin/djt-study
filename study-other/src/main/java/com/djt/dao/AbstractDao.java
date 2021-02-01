package com.djt.dao;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 数据库操作抽象类
 *
 * @author 　djt317@qq.com
 * @date 　  2021-01-29 13:54
 */
public abstract class AbstractDao {

    /**
     * 配置
     */
    protected Properties config = null;

    /**
     * 数据源
     */
    protected final DruidDataSource dataSource = new DruidDataSource();

    /**
     * 操作工具
     */
    protected QueryRunner queryRunner = new QueryRunner(dataSource);

    /**
     * 初始化数据源
     *
     * @throws Exception e
     */
    protected abstract void initDataSource() throws Exception;

    /**
     * 获取数据库连接
     *
     * @return conn
     * @throws Exception e
     */
    public Connection getConnection() throws Exception {
        if (!dataSource.isInited()) {
            initDataSource();
            dataSource.init();
        }
        return dataSource.getConnection();
    }

    /**
     * 查询
     *
     * @param sql    查询sql
     * @param params 参数列表
     * @return 结果列表
     */
    public List<Map<String, Object>> query(String sql, Object... params) throws Exception {
        return queryRunner.query(sql, new MapListHandler(), params);
    }

    /**
     * 查询
     *
     * @param sql    查询sql
     * @param params 参数列表
     * @param tClass Bean class
     * @return 结果列表
     */
    public <T> List<T> query(String sql, Class<T> tClass, Object... params) throws Exception {
        return queryRunner.query(sql, new BeanListHandler<>(tClass), params);
    }

    /**
     * 关闭 Connection
     *
     * @param connection conn
     */
    public void close(Connection connection) {
        JdbcUtils.close(connection);
    }

    /**
     * 关闭 Statement
     *
     * @param statement stmt
     */
    public void close(Statement statement) {
        JdbcUtils.close(statement);
    }

    /**
     * 关闭 ResultSet
     *
     * @param resultSet rs
     */
    public void close(ResultSet resultSet) {
        JdbcUtils.close(resultSet);
    }

    public Properties getConfig() {
        return config;
    }

    protected void setConfig(Properties config) {
        this.config = config;
    }
}
