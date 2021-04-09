package com.djt.test.dao;

import cn.hutool.core.util.StrUtil;
import cn.hutool.db.sql.SqlExecutor;
import com.alibaba.fastjson.JSONObject;
import com.djt.dao.impl.OracleDao;
import com.djt.utils.DjtConstant;
import com.djt.utils.RandomUtils;
import com.djt.utils.RegionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

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
        String startDt = "20210301";
        String endDt = "20210331";
        String table = "xdata_edw.agt_act_term_leave_1m";
        ((OracleDao) dao).createPartition(table, startDt, endDt, "M");
        truncateTable(table);
        insertBatchRandom(startDt, endDt, 1000, 1000);
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

    public void truncateTable(String table) {
        String sql = "delete from " + table;
        try {
            dao.executeSql(sql);
        } catch (SQLException throwables) {
            throw new RuntimeException("表清空失败:" + table);
        }
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
        String sql = "insert into xdata_edw.agt_act_term_leave_1m (STAT_month, BELONG_BRANCH, BRANCH_COMPANY, R_AGT_ID, R_AGT_NAME, PRODUCT_TYPE, TERM_TYPE)\n" +
                "values(?,?,?,?,?,?,?)";
        try {
            int batchCount = 0;
            List<Object[]> paramsList = new ArrayList<>();

            Map<Integer, String> pMap = new HashMap<>();
            pMap.put(1, "MPOS-LSP");
            pMap.put(2, "POSP");
            pMap.put(3, "MPOS");
            pMap.put(4, "LSPU");

            Map<Integer, String> bMap = new HashMap<>();
            bMap.put(1, "201809081044");
            bMap.put(2, "201809171081");
            bMap.put(3, "201809171082");
            bMap.put(4, "201809051024");
            bMap.put(5, "201809071028");
            bMap.put(6, "201810081101");
            bMap.put(7, "201809091001");

            for (int i = 0; i < size; i++) {
                String f1 = RandomUtils.getRandomDate(startDT, endDT, DjtConstant.YM);
                String f2 = bMap.get(RandomUtils.getRandomNumber(1, 7));
                String f3 = null;
                String f4 = RandomUtils.getString(0, 20000, 8);
                String f5 = "测试" + RandomUtils.getRandomNumber(1, 100) + "公司";
                String f6 = pMap.get(RandomUtils.getRandomNumber(1, 4));
                String f7 = null;

                Object[] params = new Object[7];

                params[0] = Long.parseLong(f1);
                params[1] = f2;
                params[2] = f3;
                params[3] = f4;
                params[4] = f5;
                params[5] = f6;
                params[6] = f7;
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
