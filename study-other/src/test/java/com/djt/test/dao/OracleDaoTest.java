package com.djt.test.dao;

import com.alibaba.fastjson.JSONObject;
import com.djt.dao.impl.OracleDao;
import com.djt.test.utils.RandomUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * @author 　djt317@qq.com
 * @date 　  2021-02-01 10:34
 */
public class OracleDaoTest extends DaoTest {

    @Override
    protected void initDao() {
        dao = OracleDao.getInstance(config);
    }


    @Test
    public void testQuery() throws Exception {
        String sql = "select * from xdata_edw.t_region_map";
        List<Map<String, Object>> beanList = dao.query(sql);
        for (Map<String, Object> data : beanList) {
            System.out.println(data);
        }
    }

    @Test
    public void testInsertBatchRandom() {
        String startDt = "2020-01-01";
        String endDt = "2020-01-31";
        insertBatchRandom(startDt, endDt, 10);
    }

    /**
     * 批量插入数据
     *
     * @param jsonObjectList 数据列表 一个json为一条数据
     */
    public void insert(List<JSONObject> jsonObjectList) {
        int batchSize = 1000;
        String sql = "insert into xdata_edw.t_region_map2 (CODE, NAME, CUP_CODE, PBOC_CODE, REGIONGRADE, PARENTREGION, LNG, LAT)\n" +
                "values (?,?,?,?,?,?,?,?)";
        PreparedStatement pstm = null;
        try {
            conn = dao.getConnection();
            pstm = conn.prepareStatement(sql);
            int batchCount = 0;
            for (int i = 0; i < jsonObjectList.size(); i++) {
                JSONObject jsonObject = jsonObjectList.get(i);
                pstm.setString(1, jsonObject.getString("CODE"));
                pstm.setString(2, jsonObject.getString("NAME"));
                pstm.setString(3, jsonObject.getString("CUP_CODE"));
                pstm.setString(4, jsonObject.getString("PBOC_CODE"));
                pstm.setString(5, jsonObject.getString("REGIONGRADE"));
                pstm.setString(6, jsonObject.getString("PARENTREGION"));
                pstm.setString(7, jsonObject.getString("LNG"));
                pstm.setString(8, jsonObject.getString("LAT"));
                pstm.addBatch();
                if ((i > 0 && i % batchSize == 0) || i == jsonObjectList.size() - 1) {
                    pstm.executeBatch();
                    pstm.clearBatch();
                    System.out.println("第 " + (++batchCount) + " 批插入成功.");
                }
            }
            System.out.println("写入总条数：" + jsonObjectList.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            dao.close(pstm);
        }
    }

    /**
     * 从文件批量插入数据
     * 文件中每行是一个insert语句
     */
    public void insertFromFile() {
        int batchSize = 1000;
        File file = new File("C:\\Users\\duanjiatao\\Desktop\\region_map.sql");
        Statement pstm = null;
        try {
            conn = dao.getConnection();
            pstm = conn.createStatement();
            List<String> lines = FileUtils.readLines(file, "UTF-8");
            int batchCount = 0;
            for (int i = 0; i < lines.size(); i++) {
                pstm.addBatch(lines.get(i));
                if ((i > 0 && i % batchSize == 0) || i == lines.size() - 1) {
                    pstm.executeBatch();
                    pstm.clearBatch();
                    System.out.println("第 " + (++batchCount) + " 批插入成功.");
                }
            }
            System.out.println("写入总条数：" + lines.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            dao.close(pstm);
        }
    }

    /**
     * 批量插入数据
     * 随机制造数据
     *
     * @param startDT 起始分区(yyyyMMdd)
     * @param endDT   截止分区(yyyyMMdd)
     * @param size    插入条数
     */
    public void insertBatchRandom(String startDT, String endDT, int size) {
        int batchSize = 100000;
        String sql = "insert into xdata_edw.BZ_MER_CHECK_TERM_TRADE_1D (TRANS_DATE,LMERCH_NO,LMERCH_NAME,LTERM_NO,PMERCH_NAME,TERM_SN,FEE_CALC_TYPE,STROKE_COUNT,AMOUNT_SUM,FEE_AMOUNT_SUM,PAYABLE_AMOUNT)\n" +
                "values(?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement pstm = null;
        try {
            pstm = conn.prepareStatement(sql);
            int batchCount = 0;
            for (int i = 0; i < size; i++) {

                /*
                  TRANS_DATE       20200601,
                  LMERCH_NO       '84931015812A00N',
                  LMERCH_NAME     '杭州宝果科技有限公司',
                  LTERM_NO        '60015518',
                  PMERCH_NAME     '杭州宝果科技有限公司',
                  TERM_SN         'N3000179885',
                  FEE_CALC_TYPE   '02',
                  STROKE_COUNT     10,
                  AMOUNT_SUM       21060000,
                  FEE_AMOUNT_SUM   113724,
                  PAYABLE_AMOUNT   20946276
                 */

                String TRANS_DATE = RandomUtils.getRandomDate(startDT, endDT, RandomUtils.YMD);
                String LMERCH_NO = RandomUtils.getString(0, 10000, 15);
                String LMERCH_NAME = "测试" + LMERCH_NO + "公司";
                String LTERM_NO = RandomUtils.getString(0, 20000, 8);
                String FEE_CALC_TYPE = RandomUtils.getString(0, 10, 2);
                long STROKE_COUNT = RandomUtils.getRandomNumber(0, 100);
                long AMOUNT_SUM = RandomUtils.getRandomNumber(0, 10000);
                long FEE_AMOUNT_SUM = RandomUtils.getRandomNumber(0, 1000);
                long PAYABLE_AMOUNT = RandomUtils.getRandomNumber(0, 1000);
                pstm.setLong(1, Long.parseLong(TRANS_DATE));
                pstm.setString(2, LMERCH_NO);
                pstm.setString(3, LMERCH_NAME);
                pstm.setString(4, LTERM_NO);
                pstm.setString(5, LMERCH_NAME);
                pstm.setString(6, LTERM_NO);
                pstm.setString(7, FEE_CALC_TYPE);
                pstm.setLong(8, STROKE_COUNT);
                pstm.setLong(9, AMOUNT_SUM);
                pstm.setLong(10, FEE_AMOUNT_SUM);
                pstm.setLong(11, PAYABLE_AMOUNT);
                pstm.addBatch();

                if ((i > 0 && i % batchSize == 0) || i == size - 1) {
                    pstm.executeBatch();
                    pstm.clearBatch();
                    System.out.println("第 " + (++batchCount) + " 批插入成功.");
                }
            }
            System.out.println("写入总条数：" + size);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            dao.close(pstm);
        }
    }
}
