package com.djt.sink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.Map;

/**
 * HBase数据
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-04
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HBaseData implements Serializable {

    /**
     * 列族
     */
    private byte[] cf = Bytes.toBytes("cf");

    /**
     * rowkey
     */
    private String rowKey;

    /**
     * 数据(k:v)
     */
    private Map<String, Object> dataMap;

    public HBaseData(String rowKey, Map<String, Object> dataMap) {
        this.rowKey = rowKey;
        this.dataMap = dataMap;
    }

}
