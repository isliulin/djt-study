package com.djt.test.dao;

import com.djt.dao.impl.MySqlDao;
import com.djt.test.bean.TestDbUtilsBean;
import com.djt.test.bean.TestHuToolBean;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.junit.Test;

import java.util.List;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-01
 */
public class MySqlDaoTest extends DaoTest {


    @Override
    protected void initDao() {
        dao = MySqlDao.getInstance(config);
    }

    @Test
    public void testQuery() throws Exception {
        String sql = "select * from xdata.t_test";
        List<TestDbUtilsBean> beanList = dao.query(sql, TestDbUtilsBean.class);
        for (TestDbUtilsBean bean : beanList) {
            System.out.println(bean);
        }
        List<TestHuToolBean> beanList2 = dao.query2(sql, TestHuToolBean.class);
        for (TestHuToolBean bean : beanList2) {
            System.out.println(bean);
        }
    }

    @Test
    public void testQueryRunner() throws Exception {
        QueryRunner queryRunner = new QueryRunner();
        String sql = "select * from xdata.t_test";
        List<TestDbUtilsBean> beanList = queryRunner.query(dao.getConnection(), sql, new BeanListHandler<>(TestDbUtilsBean.class));
        for (TestDbUtilsBean bean : beanList) {
            System.out.println(bean);
        }
    }

}
