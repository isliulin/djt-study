package com.djt.tools.test;

import cn.hutool.core.io.IoUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.Test;

import java.io.IOException;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-11-08
 */
public class OtherTest {

    @Test
    public void testOrc1() {
        Path path = new Path("C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\orc-001");
        Configuration conf = new Configuration();
        Reader reader = null;
        try {
            reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
            System.out.println("getNumberOfRows：" + reader.getNumberOfRows());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(reader);
        }
    }
}
