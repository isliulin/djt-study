package com.djt.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.db.Db;
import cn.hutool.db.DbUtil;
import cn.hutool.db.Entity;
import cn.hutool.db.sql.SqlExecutor;
import com.alibaba.druid.pool.ha.PropertiesUtils;
import com.djt.dao.impl.MySqlDao;
import com.djt.test.bean.TestHuToolBean;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.List;
import java.util.Properties;

/**
 * @author 　djt317@qq.com
 * @since  　2021-02-02
 */
public class HutoolTest {

    private final Properties config = PropertiesUtils.loadProperties("/config.properties");

    @Test
    public void testDb() throws Exception {
        MySqlDao dao = MySqlDao.getInstance(config);
        DataSource ds = dao.getDataSource();
        Db db = DbUtil.use(ds);
        List<TestHuToolBean> beanList = db.findAll(Entity.create("xdata.t_test"), TestHuToolBean.class);
        for (TestHuToolBean bean : beanList) {
            System.out.println(bean);
        }
    }

    @Test
    public void testSqlExecutor() throws Exception {
        MySqlDao dao = MySqlDao.getInstance(config);
        SqlExecutor.executeBatch(dao.getConnection(), "", "");
    }

    @Test
    public void testStrUtil() {
        String sql = StrUtil.format("ALTER TABLE {} ADD PARTITION P{} VALUES ({})", "xdata.t_test", "20210101", "20210101");
        System.out.println(sql);
    }
}
