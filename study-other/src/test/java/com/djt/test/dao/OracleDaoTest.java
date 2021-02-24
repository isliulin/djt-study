package com.djt.test.dao;

import cn.hutool.core.util.StrUtil;
import cn.hutool.db.sql.SqlExecutor;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.djt.dao.impl.OracleDao;
import com.djt.test.utils.RandomUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author 　djt317@qq.com
 * @date 　  2021-02-01 10:34
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
    public void testInsertFromFile() {
        String filePath = "C:\\Users\\duanjiatao\\Desktop\\数据中心\\我的开发\\2021\\迭代2\\地区经纬度\\t_region_map数据.sql";
        insertFromFile(filePath);
    }

    @Test
    public void testGenRegionInfo() {
        genRegionInfoByAddress();
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
     * 从文件批量插入数据
     * 文件中每行是一个insert语句
     *
     * @param filePath 文件路径
     */
    public void insertFromFile(String filePath) {
        int batchSize = 1000;
        File file = new File(filePath);
        try {
            List<String> lines = FileUtils.readLines(file, "UTF-8");
            int batchCount = 0;
            List<String> sqlList = new ArrayList<>();
            for (int i = 0; i < lines.size(); i++) {
                sqlList.add(lines.get(i));
                if ((i > 0 && i % batchSize == 0) || i == lines.size() - 1) {
                    SqlExecutor.executeBatch(conn, sqlList);
                    sqlList.clear();
                    log.info("第 {} 批插入成功.", (++batchCount));
                }
            }
            log.info("写入总条数：{}", lines.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
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
                    log.info("第 {} 批插入成功.", (++batchCount));
                }
            }
            log.info("写入总条数：{}", size);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void genRegionInfoByAddress() {
        //百度接口认证码
        String ak = "edGc5mIugVxx7lwUx9YpraKeWmExG64o";  //xxx
        //地址->经纬度
        String url = "http://api.map.baidu.com/geocoding/v3/?ak={}&address={}&output=json";
        //经纬度->地址  coordtype=wgs84ll
        String url2 = "http://api.map.baidu.com/reverse_geocoding/v3/?ak={}&output=json&coordtype=bd09ll&location={}";
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
            String responseStr = HttpUtil.get(StrUtil.format(url, ak, address));
            JSONObject regionInfo = JSON.parseObject(responseStr);
            if (regionInfo.getInteger("status") != 0) {
                log.warn("正向地址解析失败！=>{}:{}", address, responseStr);
                continue;
            }
            regionInfo = regionInfo.getJSONObject("result").getJSONObject("location");
            String lng = regionInfo.getString("lng");
            String lat = regionInfo.getString("lat");
            dataMap.put("LOG", lng);
            dataMap.put("LAT", lat);
            String location = lat + "," + lng;
            responseStr = HttpUtil.get(StrUtil.format(url2, ak, location));
            regionInfo = JSON.parseObject(responseStr);
            if (regionInfo.getInteger("status") != 0) {
                log.warn("反向地址解析失败！=>{}:{}:{}", location, address, responseStr);
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

}
