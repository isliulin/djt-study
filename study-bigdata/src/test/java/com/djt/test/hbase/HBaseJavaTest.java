package com.djt.test.hbase;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * @author 　djt317@qq.com
 * @since 　 2022-03-07
 */
@Log4j2
public class HBaseJavaTest {

    private final Configuration hbaseConf = HBaseConfiguration.create();
    private Connection conn = null;

    @Before
    public void before() {
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "cdh-dev01.jlpay.io,cdh-dev02.jlpay.io,cdh-dev03.jlpay.io");
        hbaseConf.set(HConstants.CLIENT_PORT_STR, "2181");
        try {
            conn = ConnectionFactory.createConnection(hbaseConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        if (null != conn) {
            IoUtil.close(conn);
        }
    }

    /**
     * 测试保存HBase对象的序列化与反序列化
     */
    @Test
    public void testSer() {
        String tableName = "RC:STAT_RESULT";
        Table table = null;
        byte[] cf = Bytes.toBytes("cf");
        byte[] row = Bytes.toBytes(DigestUtils.md5Hex("123456789"));
        byte[] field1 = Bytes.toBytes("testList");
        byte[] field2 = Bytes.toBytes("testSet");
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            long start = System.currentTimeMillis();
            List<String> list = new ArrayList<>();
            list.add("A");
            list.add("B");
            list.add("C");
            Set<String> set = new HashSet<>();
            set.add("1");
            set.add("2");
            set.add("3");
            Put put = new Put(row);
            put.addColumn(cf, field1, ObjectUtil.serialize(list));
            put.addColumn(cf, field2, ObjectUtil.serialize(set));
            table.put(put);
            queryRow(table, row);
            System.out.println(StrUtil.format("运行耗时: {} ms", System.currentTimeMillis() - start));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(table);
            IoUtil.close(conn);
        }
    }

    /**
     * 查询HBase
     *
     * @param table     table
     * @param family    family
     * @param row       row
     * @param qualifier qualifier
     * @param <T>       T
     * @return T
     */
    public <T> T getFieldValue(Table table, byte[] family, byte[] row, byte[] qualifier) {
        Get get = new Get(row);
        Result result;
        T ret = null;
        try {
            result = table.get(get);
            byte[] value = result.getValue(family, qualifier);
            ret = ObjectUtil.deserialize(value);
        } catch (IOException e) {
            log.error("查询HBase失败!", e);
        }
        return ret;
    }

    /**
     * 查询HBase
     *
     * @param table table
     * @param row   row
     */
    public void queryRow(Table table, byte[] row) {
        Get get = new Get(row);
        StringBuilder sb = new StringBuilder(Bytes.toString(row)).append("|");
        Result result;
        try {
            result = table.get(get);
            while (result.advance()) {
                Cell cell = result.current();
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                Object value = ObjectUtil.deserialize(CellUtil.cloneValue(cell));
                sb.append(family).append(":").append(qualifier).append("=").append(value).append("|");
            }
        } catch (IOException e) {
            log.error("查询HBase失败!", e);
        }
        System.out.println(sb);
    }

    /**
     * 查询HBase单行并将结果转为Map
     *
     * @param table table
     * @param row   row
     * @return Map<String, Object>
     */
    public Map<String, Object> getResultMap(Table table, byte[] row) {
        Map<String, Object> kvMap = new HashMap<>(0);
        try {
            Get get = new Get(row);
            Result result = table.get(get);
            result.listCells();
            while (result.advance()) {
                Cell cell = result.current();
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                Object value = ObjectUtil.deserialize(CellUtil.cloneValue(cell));
                kvMap.put(qualifier, value);
            }
        } catch (IOException e) {
            log.error("查询HBase失败!", e);
        }
        return kvMap;
    }

}
