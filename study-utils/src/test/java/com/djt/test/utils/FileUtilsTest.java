package com.djt.test.utils;

import cn.hutool.core.io.FileUtil;
import com.google.common.collect.HashMultimap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

}
