package com.djt.test.utils;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.file.PathUtil;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.HashMultimap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.orc.OrcFile;
import org.apache.hive.orc.Reader;
import org.apache.hive.orc.RecordReader;
import org.apache.hive.orc.TypeDescription;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-03-10
 */
public class FileUtilsTest {

    @Test
    public void testFile() throws IOException {
        String filePath = "C:\\Users\\duanjiatao\\Desktop\\数据中心\\我的开发\\2021\\迭代3\\apollo配置接入\\";
        String[] fileNameArr = new String[]{
                "xdata-busi-query.properties",
                "xdata-busi-report.properties",
                "xdata-dc-query.properties",
                "xdata-dc-report.properties",
                "xdata-metric.properties",
                "xdata-outer-schedule.properties",
                "xdata-push-server.properties"};

        HashMultimap<String, String> configMultimap = HashMultimap.create();

        List<Map<String, String>> fileConfMapList = new ArrayList<>();
        for (String fileName : fileNameArr) {
            List<String> lines = FileUtils.readLines(new File(filePath + fileName), "UTF-8");
            Map<String, String> confMap = new HashMap<>();
            for (String line : lines) {
                if (StringUtils.isBlank(line)) continue;
                String[] kvArr = StringUtils.split(line, "=");
                String key = kvArr[0].trim();
                String value = null;
                if (kvArr.length == 2) {
                    value = kvArr[1].trim();
                }
                confMap.put(key, value);
                configMultimap.put(key, fileName);
            }
            fileConfMapList.add(confMap);
        }

        printListMap(fileConfMapList);
        printMultimap(configMultimap);
    }

    private void printListMap(List<Map<String, String>> mapList) {
        for (Map<String, String> config : mapList) {
            System.out.println("==================================================");
            for (Map.Entry<String, String> entry : config.entrySet()) {
                System.out.println(entry.getKey() + "=" + entry.getValue());
            }
            System.out.println("\n\n\n");
        }
    }

    private void printMultimap(HashMultimap<String, String> multimap) {
        for (String key : multimap.keySet()) {
            System.out.println(key + "=" + multimap.get(key));
        }
    }

    @Test
    public void testFile2() {
        String filePath = "C:\\Users\\duanjiatao\\Desktop\\cici.txt";
        String content = FileUtil.readString(filePath, StandardCharsets.UTF_8);
        System.out.println(content);
    }

    @Test
    public void testFile3() {
        String sourcePath = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\order_txt_20220303";
        String destPath = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\order_txt_20220303_copy";
        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
            FileUtil.del(destPath);
            reader = FileUtil.getUtf8Reader(sourcePath);
            writer = FileUtil.getWriter(destPath, CharsetUtil.CHARSET_UTF_8, true);
            long counter = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(StrUtil.format("第{}行=>{}", ++counter, line));
                writer.write(line + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(reader);
            IoUtil.close(writer);
        }
    }

    @Test
    public void testOrc() {
        String filePath = "E:\\tmp\\orc_001";
        RecordReader recordReader = null;
        try {
            Path sourcePath = new Path(filePath);
            Reader orcReader = OrcFile.createReader(sourcePath, OrcFile.readerOptions(new Configuration()));
            TypeDescription schema = orcReader.getSchema();
            List<String> fieldNames = schema.getFieldNames();
            VectorizedRowBatch batch = schema.createRowBatch(1);
            recordReader = orcReader.rows();
            while (recordReader.nextBatch(batch)) {
                for (int i = 0; i < batch.size; i++) {
                    JSONObject lineJson = new JSONObject();
                    for (int j = 0; j < batch.projectionSize; j++) {
                        int projIndex = batch.projectedColumns[j];
                        ColumnVector vector = batch.cols[projIndex];
                        StringBuilder valueSb = new StringBuilder();
                        vector.stringifyValue(valueSb, i);
                        lineJson.put(fieldNames.get(projIndex), StringUtils.unwrap(valueSb.toString(), '"'));
                    }
                    System.out.println(lineJson);
                }
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
        }
    }

    @Test
    public void testReader() {
        String orc = "E:\\tmp\\txt_001";
        BufferedReader reader = FileUtil.getUtf8Reader(orc);
        Iterator<String> iter = reader.lines().iterator();
        long count = 0;
        while (iter.hasNext()) {
            ++count;
            String line = iter.next();
            System.out.println(StrUtil.format("第{}行=>{}", count, line));
            if (!iter.hasNext()) {
                System.out.println("最后一行");
            }
        }
    }

    @Test
    public void testPath() {
        String str = "E:\\aaa\\bbb\\ccc\\ddd\\eee\\fff\\orc-001";
        File file = new File(str);
        java.nio.file.Path path = file.toPath();
        System.out.println(PathUtil.getLastPathEle(path));
        System.out.println(PathUtil.getPathEle(path, -1));
        System.out.println(PathUtil.subPath(path, 0, -1));
        System.out.println(path.getParent());
        System.out.println(FileUtil.getParent(file, 1));
        System.out.println(FileUtil.file(str).getName());
        System.out.println(FileUtil.getName(str));
    }

    @Test
    public void testMove() {
        String src = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\666";
        String dest = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\777";
        FileUtil.move(FileUtil.file(src), FileUtil.file(dest), true);
    }

    public Function<String, String> mapFunc = s -> {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        return JSON.parseObject(s).getString("trans_time");
    };

    public Comparator<String> comparator = (o1, o2) -> {
        if (StringUtils.isBlank(o1) && StringUtils.isBlank(o2)) {
            return 0;
        } else if (StringUtils.isBlank(o1)) {
            return -1;
        } else if (StringUtils.isBlank(o2)) {
            return 1;
        }
        String time1 = JSON.parseObject(o1).getString("trans_time");
        String time2 = JSON.parseObject(o2).getString("trans_time");
        return StringUtils.compare(time1, time2);
    };

    @Test
    public void testParseOrcToTxt() {
        String orc = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\order_orc_20220303";
        String text = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\order_txt_20220303";
        com.djt.utils.FileUtils.parseOrcToTxt(orc, text, 100000);
        com.djt.utils.FileUtils.printFileTopLines(text, 10, null);
    }

    @Test
    public void testCopyLines() {
        String filePath = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\order_txt_20220303";
        String destPath = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\test_txt";
        com.djt.utils.FileUtils.copyLines(filePath, destPath, 10000);
        com.djt.utils.FileUtils.printFileTopLines(destPath, 10, mapFunc);
    }

    @Test
    public void testSortFileByLine() {
        String srcPath = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\order_txt_20220303";
        String descPath = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\order_txt_20220303_sorted";
        com.djt.utils.FileUtils.sortFileByLine(srcPath, descPath, 100000, comparator, true);
    }

    @Test
    public void testPrintFileTopLines() {
        String path = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\order_txt_20220303_sorted";
        com.djt.utils.FileUtils.printFileTopLines(path, 600 * 10000, mapFunc);
    }

    @Test
    public void testCopySortPrint() {
        testCopyLines();
        testSortFileByLine();
        testPrintFileTopLines();
    }

    @Test
    public void testMergeSort() {
        String file1 = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\test_sort_1";
        String file2 = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\test_sort_2";
        String dest = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\test_sort_result";
        com.djt.utils.FileUtils.mergeSort(file1, file2, dest, comparator, true);
        com.djt.utils.FileUtils.printFileTopLines(dest, 500 * 10000, mapFunc);
    }

}
