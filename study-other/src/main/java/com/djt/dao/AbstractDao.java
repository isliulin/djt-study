package com.djt.dao;

import cn.hutool.db.Db;
import cn.hutool.db.DbUtil;
import cn.hutool.db.Entity;
import cn.hutool.db.sql.SqlExecutor;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 数据库操作抽象类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-01-29
 */
@Slf4j
public abstract class AbstractDao {

    /**
     * 数据源
     */
    private final DruidDataSource dataSource = new DruidDataSource();

    /**
     * 数据库操作工具
     */
    protected QueryRunner queryRunner;

    /**
     * 数据库操作工具2
     */
    protected Db db;

    protected AbstractDao(Properties config) {
        initDataSource(dataSource, config);
        queryRunner = new QueryRunner(dataSource);
        db = DbUtil.use(dataSource);
    }

    /**
     * 初始化数据源
     *
     * @param dataSource 数据源
     * @param config     配置
     */
    protected abstract void initDataSource(DruidDataSource dataSource, Properties config);

    public DruidDataSource getDataSource() {
        return dataSource;
    }

    /**
     * 获取数据库连接
     *
     * @return conn
     * @throws SQLException e
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * 查询
     *
     * @param sql    查询sql
     * @param params 参数列表
     * @return 结果列表
     * @throws SQLException e
     */
    public List<Map<String, Object>> query(String sql, Object... params) throws SQLException {
        return queryRunner.query(sql, new MapListHandler(), params);
    }

    /**
     * 查询
     *
     * @param sql    查询sql
     * @param params 参数列表
     * @return 结果列表
     * @throws SQLException e
     */
    public List<Entity> query2(String sql, Object... params) throws SQLException {
        return db.query(sql, params);
    }

    /**
     * 查询
     *
     * @param sql    查询sql
     * @param params 参数列表
     * @param tClass Bean class
     * @return 结果列表
     * @throws SQLException e
     */
    public <T> List<T> query(String sql, Class<T> tClass, Object... params) throws SQLException {
        return queryRunner.query(sql, new BeanListHandler<>(tClass), params);
    }

    /**
     * 查询
     *
     * @param sql    查询sql
     * @param params 参数列表
     * @param tClass Bean class
     * @return 结果列表
     * @throws SQLException e
     */
    public <T> List<T> query2(String sql, Class<T> tClass, Object... params) throws SQLException {
        return db.query(sql, tClass, params);
    }

    /**
     * 执行SQL
     *
     * @param sql 待执行的SQL
     * @throws SQLException e
     */
    public void executeSql(String sql) throws SQLException {
        Validate.notBlank(sql, "SQL不能为空！");
        db.execute(sql);
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

    /**
     * 从文件批量执行SQL
     * 文件中每行是一个SQL语句
     *
     * @param filePath 文件路径
     */
    public void executeBatchFromFile(String filePath, int batchSize) {
        File file = new File(filePath);
        Connection conn = null;
        try {
            conn = getConnection();
            List<String> lines = FileUtils.readLines(file, "UTF-8");
            int batchCount = 0;
            List<String> sqlList = new ArrayList<>();
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                //若行尾有分号需要自动剔除
                if (StringUtils.endsWith(line, ";")) {
                    line = StringUtils.removeEnd(line, ";");
                }
                sqlList.add(line);
                boolean isExecute = (i > 0 && i % batchSize == 0) || i == lines.size() - 1;
                if (isExecute) {
                    SqlExecutor.executeBatch(conn, sqlList);
                    sqlList.clear();
                    log.info("第 {} 批执行成功.", (++batchCount));
                }
            }
            log.info("写入总条数：{}", lines.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            close(conn);
        }
    }

}
