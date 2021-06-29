package com.djt.test.dao;

import cn.hutool.db.Entity;
import com.alibaba.druid.util.JdbcUtils;
import com.djt.dao.impl.HiveDao;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-06-28
 */
public class HiveDaoTest extends DaoTest {

    @Override
    protected void initDao() {
        dao = HiveDao.getInstance(config);
    }

    @Test
    public void testQuery() throws Exception {
        String sql = "select 1";
        List<Entity> mapList = dao.query2(sql);
        for (Entity entity : mapList) {
            System.out.println(entity);
        }
    }

    @Test
    public void testJdbcUtils() throws SQLException {
        String sql = "show schemas";
        List<Map<String, Object>> result = JdbcUtils.executeQuery(dao.getDataSource(), sql);
        for (Map<String, Object> map : result) {
            System.out.println(map);
        }
    }


}
