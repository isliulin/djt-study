package com.djt.utils;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author 　djt317@qq.com
 * @since 　 2022-04-15
 */
public class FileData {

    private final static String FILKE_PATH = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\merch_list";
    public final static List<String> MERCH_LIST = new ArrayList<>();

    static {
        BufferedReader reader = null;
        try {
            reader = FileUtil.getUtf8Reader(FILKE_PATH);
            Iterator<String> iter = reader.lines().iterator();
            while (iter.hasNext()) {
                MERCH_LIST.add(iter.next());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(reader);
        }
    }

}
