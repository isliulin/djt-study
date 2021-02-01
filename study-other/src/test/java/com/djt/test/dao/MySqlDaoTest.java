package com.djt.test.dao;

import com.djt.dao.impl.MySqlDao;
import com.djt.test.bean.TestBean;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.junit.Test;

import java.util.List;

/**
 * @author 　djt317@qq.com
 * @date 　  2021-02-01 10:34
 */
public class MySqlDaoTest extends DaoTest {


    @Override
    protected void initDao() {
        dao = MySqlDao.getInstance(config);
    }

    @Test
    public void testQuery() throws Exception {
        String sql = "select * from xdata.t_test";
        List<TestBean> beanList = dao.query(sql, TestBean.class);
        for (TestBean bean : beanList) {
            System.out.println(bean);
        }
    }

    @Test
    public void testQueryRunner() throws Exception {
        QueryRunner queryRunner = new QueryRunner();
        String sql = "select * from xdata.t_test";
        List<TestBean> beanList = queryRunner.query(dao.getConnection(), sql, new BeanListHandler<>(TestBean.class));
        for (TestBean bean : beanList) {
            System.out.println(bean);
        }
    }

}
