package com.djt.tools.impl;

import com.djt.tools.AbsTools;
import com.djt.utils.DjtConstant;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * HDFS临时工具
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-09
 */
public class HdfsTools extends AbsTools {

    private static final Logger log = LogManager.getLogger(HdfsTools.class);

    /**
     * 扫描指定路径下的文件 根据条件过滤
     */
    @Override
    public void doExecute(String[] args) throws Exception {
        String hdfsUrl = PROPS.getStr("hdfs.url");
        Validate.notBlank(hdfsUrl, "hdfs.url不能为空！");
        String today = LocalDate.now().format(DjtConstant.YMD);

        FileSystem hdfs = FileSystem.get(URI.create(hdfsUrl), new Configuration());
        Path basePath = new Path("/user/hive/warehouse/");
        RemoteIterator<LocatedFileStatus> fileStatuseItor = hdfs.listFiles(basePath, true);
        File resultFile = new File("./result.txt");
        FileUtils.deleteQuietly(resultFile);

        while (fileStatuseItor.hasNext()) {
            LocatedFileStatus fileStatus = fileStatuseItor.next();
            if (fileStatus.isDirectory()) {
                continue;
            }
            Path filePath = fileStatus.getPath();
            String fileName = filePath.getName();
            long modTime = fileStatus.getModificationTime();
            Instant instant = Instant.ofEpochSecond(modTime / 1000);
            String dateTime = LocalDateTime.ofInstant(instant, DjtConstant.ZONE_ID).format(DjtConstant.YMD);
            if (!today.equals(dateTime) || StringUtils.startsWithAny(fileName, "ORC", "part", "_SUCCESS", ".")) {
                continue;
            }

            String line = dateTime + "\t" + filePath.toString() + "\n";
            FileUtils.write(resultFile, line, true);
            log.info(line);
        }
    }


}
