package com.djt.test.dao;

import com.alibaba.druid.pool.ha.PropertiesUtils;
import com.djt.dao.AbstractDao;
import com.djt.dao.impl.MySqlDao;
import com.djt.dao.impl.OracleDao;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.Properties;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-01-29
 */
public abstract class DaoTest {

    protected Properties config;
    protected AbstractDao dao;
    protected Connection conn;

    /**
     * 初始化Dao 由子类实现
     */
    abstract protected void initDao();

    @Before
    public void before() {
        config = PropertiesUtils.loadProperties("/config.properties");
        System.out.println("=======初始化开始=======");
        System.out.println(config.toString());
        initDao();
        if (dao == null) {
            throw new IllegalArgumentException("dao不能为空，请初始化！");
        }
        if (conn == null) {
            try {
                conn = dao.getConnection();
            } catch (Exception e) {
                throw new RuntimeException("获取数据库连接失败！", e);
            }
        }
        System.out.println("=======初始化完成=======");
    }

    @After
    public void after() {
        if (dao != null) {
            dao.close(conn);
            System.out.println("=======数据库连接关闭=======");
        }
    }

    @Test
    public void testMySqlDao() throws Exception {
        MySqlDao dao = MySqlDao.getInstance(config);
        Connection conn = dao.getConnection();
        System.out.println("创建连接成功...");
        dao.close(conn);
        System.out.println("关闭连接成功...");
    }

    @Test
    public void testOracleDao() throws Exception {
        OracleDao dao = OracleDao.getInstance(config);
        Connection conn = dao.getConnection();
        System.out.println("创建连接成功...");
        dao.close(conn);
        System.out.println("关闭连接成功...");
    }

    @Test
    public void testOther() {

    }

}
