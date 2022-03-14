package com.djt.utils;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;

/**
 * 文件工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2022-03-10
 */
@Log4j2
public class FileUtils {

    /**
     * ORC文件转换为txt文件
     *
     * @param sourceOrcPath 源文件
     * @param destTxtPath   目标文件
     * @param isAppend      是否追加
     */
    public static void parseOrcToTxt(String sourceOrcPath, String destTxtPath, boolean isAppend) {
        long start = System.currentTimeMillis();
        RecordReader recordReader = null;
        BufferedWriter txtWriter = null;
        try {
            if (!isAppend) {
                FileUtil.del(destTxtPath);
            }
            Path sourcePath = new Path(sourceOrcPath);
            Reader orcReader = OrcFile.createReader(sourcePath, OrcFile.readerOptions(new Configuration()));
            txtWriter = FileUtil.getWriter(destTxtPath, CharsetUtil.CHARSET_UTF_8, true);
            StructObjectInspector inspector = (StructObjectInspector) orcReader.getObjectInspector();
            List<? extends StructField> structFields = inspector.getAllStructFieldRefs();
            recordReader = orcReader.rows();
            Object row = null;
            while (recordReader.hasNext()) {
                row = recordReader.next(row);
                JSONObject jsonObject = new JSONObject();
                for (StructField structField : structFields) {
                    String name = structField.getFieldName();
                    Object value = inspector.getStructFieldData(row, structField);
                    value = value == null ? null : value.toString();
                    jsonObject.put(name, value);
                }
                txtWriter.write(jsonObject.toString() + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (recordReader != null) {
                try {
                    recordReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            IOUtils.closeQuietly(txtWriter);
        }
        long stop = System.currentTimeMillis();
        log.info("文件转换完成,耗时: {} s", (stop - start) / 1000d);
    }

    /**
     * 打印文件前N行
     *
     * @param filePath 文件路径
     * @param lines    打印行数
     */
    public static void printFileTopLines(String filePath, long lines) {
        BufferedReader reader = null;
        Validate.isTrue(lines > 0, "行数必须大于0");
        long lineCount = 0;
        try {
            reader = FileUtil.getUtf8Reader(filePath);
            String line;
            while ((line = reader.readLine()) != null) {
                if (++lineCount > lines) {
                    return;
                }
                System.out.println(StrUtil.format("第{}行=>{}", lineCount, line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(reader);
        }
    }

}
