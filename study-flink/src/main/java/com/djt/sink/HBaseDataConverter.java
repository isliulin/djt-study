package com.djt.sink;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.connector.hbase.sink.HBaseMutationConverter;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase数据转换 用于Sink
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-18
 */
public class HBaseDataConverter implements HBaseMutationConverter<HBaseData> {

    /**
     * 空Put 防止返回null造成NullPointerException
     */
    private Put emptyPut;

    @Override
    public void open() {
        if (null == emptyPut) {
            emptyPut = new Put(Bytes.toBytes("err_data"));
            emptyPut.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(""), Bytes.toBytes(""));
        }
    }

    @Override
    public Mutation convertToMutation(HBaseData record) {
        if (null == record.getRowKey()) {
            return emptyPut;
        }
        Put put = new Put(Bytes.toBytes(DigestUtils.md5Hex(record.getRowKey())));
        record.getDataMap().forEach((qualifier, value) -> {
            if (null != qualifier && null != value) {
                put.addColumn(record.getCf(), Bytes.toBytes(qualifier), Bytes.toBytes(value.toString()));
            }
        });
        if (put.isEmpty()) {
            return emptyPut;
        }
        return put;
    }
}
