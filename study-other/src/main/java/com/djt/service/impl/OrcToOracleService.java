package com.djt.service.impl;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.lang.UUID;
import com.djt.dao.impl.OracleDao;
import com.djt.service.AbstractService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 风控规则抽象类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-10-28
 */
public class OrcToOracleService extends AbstractService {

    private final OracleDao dao;

    public OrcToOracleService() {
        dao = OracleDao.getInstance(config);
    }

    @Override
    protected void updateConfig(Properties config) {
        config.put("oracle.druid.Url", "jdbc:oracle:thin:@172.20.6.22:1521:mpos");
        config.put("oracle.druid.Username", "order_user");
        config.put("oracle.druid.Password", "e1c9599010ace999c41700ab183d9fe9");
    }

    public void readOrcToOracle(String filePath, String date, String table) {
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        Reader reader = null;
        RecordReader rows = null;
        try {
            reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
            rows = reader.rows();
            TypeDescription schema = reader.getSchema();
            List<TypeDescription> children = schema.getChildren();
            VectorizedRowBatch batch = schema.createRowBatch(10000);
            int numberOfChildren = children.size();
            while (rows.nextBatch(batch)) {
                List<OrcStruct> resultList = new ArrayList<>();
                for (int r = 0; r < batch.size; r++) {
                    OrcStruct result = new OrcStruct(schema);
                    for (int i = 0; i < numberOfChildren; ++i) {
                        result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], 1,
                                children.get(i), result.getFieldValue(i)));
                    }
                    resultList.add(result);
                }
                insertOracle(resultList, date, table);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(rows);
            IoUtil.close(reader);
        }
    }

    private void insertOracle(List<OrcStruct> resultList, String date, String table) {
        if (CollectionUtils.isEmpty(resultList)) {
            return;
        }
        List<String> sqlList = new ArrayList<>();
        for (OrcStruct struct : resultList) {
            List<String> names = struct.getSchema().getFieldNames();
            StringBuilder fieldName = new StringBuilder();
            StringBuilder fieldValue = new StringBuilder();
            for (int i = 0; i < names.size(); i++) {
                String name = names.get(i);
                String value = String.valueOf(struct.getFieldValue(i));
                if ("order_id".equalsIgnoreCase(name)) {
                    value = UUID.randomUUID().toString(true);
                }
                if (name.endsWith("_time")) {
                    value = date + " " + StringUtils.substringAfter(value, " ");
                    value = "to_date('" + value + "','yyyy-mm-dd hh24:mi:ss')";
                } else if (value == null || "null".equalsIgnoreCase(value)) {
                    value = null;
                } else {
                    value = StringUtils.wrap(value, "'");
                }


                fieldName.append(name);
                fieldValue.append(value);
                if (i != names.size() - 1) {
                    fieldName.append(",");
                    fieldValue.append(",");
                }
            }
            String sql = "insert into " + table +
                    " (" + fieldName.toString() + ") " +
                    "values (" + fieldValue.toString() + ")";
            sqlList.add(sql);
        }
        dao.executeBatch(sqlList, sqlList.size());
    }

}
