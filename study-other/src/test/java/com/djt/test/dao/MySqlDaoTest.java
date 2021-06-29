package com.djt.test.dao;

import cn.hutool.db.sql.SqlExecutor;
import com.alibaba.druid.util.JdbcUtils;
import com.djt.dao.impl.MySqlDao;
import com.djt.test.bean.TestDbUtilsBean;
import com.djt.test.bean.TestHuToolBean;
import com.djt.utils.RandomUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-01
 */
@Slf4j
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

    @Test
    public void testBatchInsert() {
        int totalSize = 100000;
        int batchSize = 10000;
        String startDate = "20201201";
        String endDate = "20201231";
        String sql = "INSERT INTO test.student (name, sex, age, is_del, remark, create_time, update_time)\n" +
                "VALUES(?, ?, ?, ?, ?, ?, ?)";
        try {
            int batchCount = 0;
            List<Object[]> paramsList = new ArrayList<>();
            for (int i = 0; i < totalSize; i++) {

                /*
                  name         张三,
                  sex          1-男 2-女,
                  age          1-100,
                  is_del       0/1,
                  create_time  2021-03-05 12:34:56,
                  update_time  2021-03-05 12:34:56,
                 */

                long sex = RandomUtils.getRandomNumber(1, 2);
                String name = RandomUtils.getRandomName((byte) sex);
                long age = RandomUtils.getRandomNumber(1, 80);
                long isDel = RandomUtils.getRandomNumber(0, 1);
                String createTime = RandomUtils.getRandomDate(startDate, endDate);

                Object[] params = new Object[7];
                params[0] = name;
                params[1] = sex;
                params[2] = age;
                params[3] = isDel;
                params[4] = null;
                params[5] = createTime;
                params[6] = createTime;
                paramsList.add(params);

                if ((i > 0 && i % batchSize == 0) || i == totalSize - 1) {
                    SqlExecutor.executeBatch(conn, sql, paramsList);
                    paramsList.clear();
                    log.info("第 {} 批插入成功.", (++batchCount));
                }
            }
            log.info("写入总条数：{}", totalSize);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testJdbcUtils() throws SQLException {
        String sql = "select\n" +
                "t1.*\n" +
                "from etl_db.t_ogg_kafka_config t1\n" +
                "left join etl_db.t_rl_project_table_config t2 on t1.table_id =t2.table_id \n" +
                "left join etl_db.t_rl_project_info_config t3 on t2.task_id = t3.task_id\n" +
                "where t3.project_name ='xdata-realtime-stream'";
        List<Map<String, Object>> result = JdbcUtils.executeQuery(dao.getDataSource(), sql);
        for (Map<String, Object> map : result) {
            System.out.println(map);
        }
    }

}
