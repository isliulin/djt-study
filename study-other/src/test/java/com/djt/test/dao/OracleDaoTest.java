package com.djt.test.dao;

import cn.hutool.core.util.StrUtil;
import cn.hutool.db.sql.SqlExecutor;
import com.alibaba.fastjson.JSONObject;
import com.djt.dao.impl.OracleDao;
import com.djt.utils.RandomUtils;
import com.djt.utils.RegionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-01
 */
@Slf4j
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
            log.info(data.toString());
        }
    }

    @Test
    public void testInsertBatchRandom() {
        String startDt = "20201201";
        String endDt = "20201231";
        ((OracleDao) dao).createPartition("xdata_edw.bz_mer_check_term_trade_1d", startDt, endDt);
        insertBatchRandom(startDt, endDt, 100, 10);
    }

    @Test
    public void testResetRegionMap() throws SQLException {
        String sql = "delete from xdata_edw.t_region_map";
        dao.executeSql(sql);
        String filePath = "C:\\Users\\duanjiatao\\Desktop\\数据中心\\我的开发\\2021\\迭代2\\地区经纬度\\t_region_map数据.sql";
        dao.executeBatchFromFile(filePath, 1000);
    }

    @Test
    public void testGenRegionInfo() {
        genRegionInfo();
    }

    @Test
    public void testUpdateRegionInfo() {
        //String filePath = "C:\\Users\\duanjiatao\\Desktop\\update_tmp.sql";
        //dao.executeBatchFromFile(filePath, 1000);
        updateRegionInfo();
    }

    /**
     * 批量插入数据
     * 原生写法
     *
     * @param dataList 数据列表
     */
    public void insertRegionInfo(List<? extends Map<String, Object>> dataList) {
        int batchSize = 1000;
        String clearSql = "delete from xdata_edw.t_region_map2";
        String sql = "insert into xdata_edw.t_region_map2 (CODE, NAME, CUP_CODE, PBOC_CODE, REGIONGRADE, PARENTREGION, LOG, LAT, ALL_NAME_LEFT, ALL_NAME_RIGHT)\n" +
                "values (?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement pstm = null;
        try {
            conn = dao.getConnection();
            pstm = conn.prepareStatement(sql);
            dao.executeSql(clearSql);
            int batchCount = 0;
            for (int i = 0; i < dataList.size(); i++) {
                JSONObject jsonObject = new JSONObject(dataList.get(i));
                pstm.setString(1, jsonObject.getString("CODE"));
                pstm.setString(2, jsonObject.getString("NAME"));
                pstm.setString(3, jsonObject.getString("CUP_CODE"));
                pstm.setString(4, jsonObject.getString("PBOC_CODE"));
                pstm.setString(5, jsonObject.getString("REGIONGRADE"));
                pstm.setString(6, jsonObject.getString("PARENTREGION"));
                pstm.setString(7, jsonObject.getString("LOG"));
                pstm.setString(8, jsonObject.getString("LAT"));
                pstm.setString(9, jsonObject.getString("ALL_NAME_LEFT"));
                pstm.setString(10, jsonObject.getString("ALL_NAME_RIGHT"));
                pstm.addBatch();
                if ((i > 0 && i % batchSize == 0) || i == dataList.size() - 1) {
                    pstm.executeBatch();
                    pstm.clearBatch();
                    log.info("第 {} 批插入成功.", (++batchCount));
                }
            }
            log.info("写入总条数：{}", dataList.size());
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
     * @param startDT   起始分区(yyyyMMdd)
     * @param endDT     截止分区(yyyyMMdd)
     * @param size      插入条数
     * @param batchSize 分批大小
     */
    public void insertBatchRandom(String startDT, String endDT, int size, int batchSize) {
        String sql = "insert into xdata_edw.BZ_MER_CHECK_TERM_TRADE_1D (TRANS_DATE,LMERCH_NO,LMERCH_NAME,LTERM_NO,PMERCH_NAME,TERM_SN,FEE_CALC_TYPE,STROKE_COUNT,AMOUNT_SUM,FEE_AMOUNT_SUM,PAYABLE_AMOUNT)\n" +
                "values(?,?,?,?,?,?,?,?,?,?,?)";
        try {
            int batchCount = 0;
            List<Object[]> paramsList = new ArrayList<>();
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
                Object[] params = new Object[11];

                params[0] = Long.parseLong(TRANS_DATE);
                params[1] = LMERCH_NO;
                params[2] = LMERCH_NAME;
                params[3] = LTERM_NO;
                params[4] = LMERCH_NAME;
                params[5] = LTERM_NO;
                params[6] = FEE_CALC_TYPE;
                params[7] = STROKE_COUNT;
                params[8] = AMOUNT_SUM;
                params[9] = FEE_AMOUNT_SUM;
                params[10] = PAYABLE_AMOUNT;
                paramsList.add(params);

                if ((i > 0 && i % batchSize == 0) || i == size - 1) {
                    SqlExecutor.executeBatch(conn, sql, paramsList);
                    paramsList.clear();
                    log.info("第 {} 批插入成功.", (++batchCount));
                }
            }
            log.info("写入总条数：{}", size);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据 t_region_map 生成 t_region_map2
     */
    public void genRegionInfo() {
        String sql = "SELECT t1.*,\n" +
                "       replace(t3.name||(case when t2.name<>t3.name then t2.name else '' end)||(case when t1.name<>t2.name then t1.name else '' end),' ','') AS all_name_left\n" +
                "FROM xdata_edw.t_region_map t1\n" +
                "LEFT JOIN xdata_edw.t_region_map t2 ON t1.parentregion = t2.code\n" +
                "LEFT JOIN xdata_edw.t_region_map t3 ON t2.parentregion = t3.code";
        List<Map<String, Object>> resultList;
        try {
            resultList = dao.query(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        for (Map<String, Object> dataMap : resultList) {
            String address = Optional.ofNullable(dataMap.get("ALL_NAME_LEFT")).orElse("").toString();
            if (StringUtils.isBlank(address)) {
                continue;
            }

            JSONObject regionInfo = RegionUtils.getRegionByAddress(address);
            if (regionInfo.getInteger("status") != 0) {
                log.warn("正向地址解析失败！=>{}:{}", address, regionInfo);
                continue;
            }
            regionInfo = regionInfo.getJSONObject("result").getJSONObject("location");
            String lng = regionInfo.getString("lng");
            String lat = regionInfo.getString("lat");
            dataMap.put("LOG", lng);
            dataMap.put("LAT", lat);
            regionInfo = RegionUtils.getRegionByLngLat(lng, lat);
            if (regionInfo.getInteger("status") != 0) {
                log.warn("反向地址解析失败！=>{},{}:{}:{}", lng, lat, address, regionInfo);
                continue;
            }
            regionInfo = regionInfo.getJSONObject("result").getJSONObject("addressComponent");
            String province = regionInfo.getString("province");
            String city = regionInfo.getString("city");
            String district = regionInfo.getString("district");
            String adcode = regionInfo.getString("adcode");
            dataMap.put("ALL_NAME_RIGHT", province + city + district + "&adcode=" + adcode);
        }
        insertRegionInfo(resultList);
    }

    /**
     * 更新 t_region_map2 的 all_name_right 字段
     */
    public void updateRegionInfo() {
        String selectSql = "SELECT code,log,lat FROM xdata_edw.t_region_map2";
        List<Map<String, Object>> resultList;
        try {
            resultList = dao.query(selectSql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String updateSql = "UPDATE xdata_edw.t_region_map2 SET all_name_right='{}' WHERE code='{}'";
        List<String> updateSqlList = new ArrayList<>();
        for (Map<String, Object> dataMap : resultList) {
            String code = Optional.ofNullable(dataMap.get("CODE")).orElse("").toString();
            String lng = Optional.ofNullable(dataMap.get("LOG")).orElse("").toString();
            String lat = Optional.ofNullable(dataMap.get("LAT")).orElse("").toString();
            if (StringUtils.isAnyBlank(code, lng, lat)) {
                continue;
            }
            JSONObject regionInfo = RegionUtils.getRegionByLngLat(lng, lat);
            if (regionInfo.getInteger("status") != 0) {
                log.warn("反向地址解析失败！=>{},{}:{}", lng, lat, regionInfo);
                continue;
            }
            regionInfo = regionInfo.getJSONObject("result").getJSONObject("addressComponent");
            String province = regionInfo.getString("province");
            String city = regionInfo.getString("city");
            String district = regionInfo.getString("district");
            String adcode = regionInfo.getString("adcode");
            String allNameRight = province + city + district + "&adcode=" + adcode;
            String sql = StrUtil.format(updateSql, allNameRight, code);
            updateSqlList.add(sql);
        }
        try {
            SqlExecutor.executeBatch(conn, updateSqlList);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }


}
