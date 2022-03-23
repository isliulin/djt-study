package com.djt.utils;

import cn.hutool.core.comparator.CompareUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.orc.OrcFile;
import org.apache.hive.orc.Reader;
import org.apache.hive.orc.RecordReader;
import org.apache.hive.orc.TypeDescription;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

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
     * @param batchSize     批次大小
     */
    public static void parseOrcToTxt(String sourceOrcPath, String destTxtPath, int batchSize) {
        Validate.isTrue(FileUtil.isFile(sourceOrcPath), "文件不存在: {}", sourceOrcPath);
        System.out.println("文件转换开始");
        long start = System.currentTimeMillis();
        RecordReader recordReader = null;
        BufferedWriter txtWriter = null;
        String tmpFile = destTxtPath + "_" + UUID.randomUUID();
        try {
            Path sourcePath = new Path(sourceOrcPath);
            Reader orcReader = OrcFile.createReader(sourcePath, OrcFile.readerOptions(new Configuration()));
            txtWriter = FileUtil.getWriter(tmpFile, CharsetUtil.CHARSET_UTF_8, true);
            TypeDescription schema = orcReader.getSchema();
            List<String> fieldNames = schema.getFieldNames();
            VectorizedRowBatch batch = schema.createRowBatch(batchSize);
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
                    txtWriter.write(lineJson.toString() + "\n");
                }
            }
            IoUtil.close(txtWriter);
            FileUtil.move(FileUtil.file(tmpFile), FileUtil.file(destTxtPath), true);
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
            FileUtil.del(tmpFile);
        }
        long stop = System.currentTimeMillis();
        System.out.println(StrUtil.format("文件转换完成,耗时: {} s", (stop - start) / 1000d));
    }

    /**
     * 复制文件前N行
     *
     * @param srcPath  源文件
     * @param destPath 目标文件
     * @param lines    行数
     */
    public static void copyLines(String srcPath, String destPath, long lines) {
        Validate.isTrue(FileUtil.isFile(srcPath), "文件不存在: {}", srcPath);
        System.out.println("文件复制开始");
        long start = System.currentTimeMillis();
        BufferedReader reader = null;
        BufferedWriter writer = null;
        String tmpFile = destPath + "_" + UUID.randomUUID();
        try {
            reader = FileUtil.getUtf8Reader(srcPath);
            writer = FileUtil.getWriter(tmpFile, StandardCharsets.UTF_8, true);
            long count = 0;
            Iterator<String> iter = reader.lines().iterator();
            while (iter.hasNext()) {
                writer.write(iter.next() + "\n");
                if (++count >= lines) {
                    break;
                }
            }
            IoUtil.close(writer);
            FileUtil.move(FileUtil.file(tmpFile), FileUtil.file(destPath), true);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(reader);
            IoUtil.close(writer);
            FileUtil.del(tmpFile);
        }
        long stop = System.currentTimeMillis();
        System.out.println(StrUtil.format("文件复制结束,耗时: {} s", (stop - start) / 1000d));
    }

    /**
     * 按照自定义格式打印文件前N行
     *
     * @param filePath 文件路径
     * @param lines    打印行数
     * @param mapFunc  数据转换函数
     */
    public static void printFileTopLines(String filePath, long lines, Function<String, String> mapFunc) {
        Validate.isTrue(FileUtil.isFile(filePath), "文件不存在: {}", filePath);
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
                if (mapFunc != null) {
                    line = mapFunc.apply(line);
                }
                System.out.println(StrUtil.format("第{}行=>{}", lineCount, line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(reader);
        }
    }

    /**
     * 将文件按照行排序
     *
     * @param srcPath    源文件
     * @param destPath   目标文件
     * @param batchSize  批次大小
     * @param comparator 行比较器
     * @param isAsc      是否升序
     */
    public static void sortFileByLine(String srcPath, String destPath, long batchSize,
                                      Comparator<String> comparator, boolean isAsc) {
        Validate.isTrue(FileUtil.isFile(srcPath), "文件不存在: {}", srcPath);
        System.out.println("文件排序开始");
        long start = System.currentTimeMillis();
        long stop;
        Validate.isTrue(FileUtil.isFile(srcPath), "源文件不存在！");
        BufferedReader reader = null;
        String tmpDir = FileUtil.getParent(srcPath, 1) + "\\.tmp_" + UUID.randomUUID();
        try {
            FileUtil.mkdir(tmpDir);
            reader = FileUtil.getUtf8Reader(srcPath);
            List<String> sortFileList = new ArrayList<>();
            List<String> tmpLineList = new ArrayList<>();
            long batchCount = 0;
            System.out.println("文件拆分开始");
            //切分大文件为排序后的小文件
            Iterator<String> lineIter = reader.lines().iterator();
            while (lineIter.hasNext()) {
                tmpLineList.add(lineIter.next());
                if ((++batchCount) % batchSize == 0 || !lineIter.hasNext()) {
                    tmpLineList.sort(comparator);
                    String tmpFile = tmpDir + "\\" + UUID.randomUUID();
                    FileUtil.writeLines(tmpLineList, tmpFile, StandardCharsets.UTF_8);
                    sortFileList.add(tmpFile);
                    tmpLineList.clear();
                }
            }
            stop = System.currentTimeMillis();
            System.out.println(StrUtil.format("文件拆分结束,耗时: {} s", (stop - start) / 1000d));

            int threadNum = (int) Math.ceil(sortFileList.size() / 2d);
            ExecutorService executor = ThreadUtil.newExecutor(threadNum, threadNum, sortFileList.size());
            //并行归并排序
            while (sortFileList.size() > 1) {
                List<String> tmpFileList = Collections.synchronizedList(new ArrayList<>());
                List<Future<?>> futureList = new ArrayList<>();
                Iterator<String> fileIter = sortFileList.iterator();
                //文件两两归并排序
                while (fileIter.hasNext()) {
                    String fileA = fileIter.next();
                    if (fileIter.hasNext()) {
                        fileIter.remove();
                        String fileB = fileIter.next();
                        fileIter.remove();
                        Future<?> future = executor.submit(() -> {
                            String tmpFile = tmpDir + "\\" + UUID.randomUUID();
                            mergeSort(fileA, fileB, tmpFile, comparator, isAsc);
                            FileUtil.del(fileA);
                            FileUtil.del(fileB);
                            tmpFileList.add(tmpFile);
                        });
                        futureList.add(future);
                    }
                }
                for (Future<?> future : futureList) {
                    try {
                        future.get(Long.MAX_VALUE, TimeUnit.SECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        throw new RuntimeException("文件合并失败！", e);
                    }
                }
                sortFileList.addAll(tmpFileList);
            }
            executor.shutdown();
            //最终文件改名
            FileUtil.move(FileUtil.file(sortFileList.get(0)), FileUtil.file(destPath), true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(reader);
            FileUtil.del(tmpDir);
        }
        stop = System.currentTimeMillis();
        System.out.println(StrUtil.format("文件排序完成,耗时: {} s", (stop - start) / 1000d));
    }

    /**
     * 两个文件合并排序(源文件必须已经排好序)
     *
     * @param fileA      文件A
     * @param fileB      文件B
     * @param destFile   目标文件
     * @param comparator 行比较器
     * @param isAsc      是否升序
     */
    public static void mergeSort(String fileA, String fileB, String destFile,
                                 Comparator<String> comparator, boolean isAsc) {
        Validate.isTrue(FileUtil.isFile(fileA), "文件不存在: {}", fileA);
        Validate.isTrue(FileUtil.isFile(fileB), "文件不存在: {}", fileB);
        System.out.println("文件合并开始");
        long start = System.currentTimeMillis();
        BufferedReader readerA = null;
        BufferedReader readerB = null;
        BufferedWriter writer = null;
        String tmpFile = destFile + "_" + UUID.randomUUID();
        try {
            readerA = FileUtil.getUtf8Reader(fileA);
            readerB = FileUtil.getUtf8Reader(fileB);
            writer = FileUtil.getWriter(tmpFile, CharsetUtil.CHARSET_UTF_8, true);
            Iterator<String> iterA = readerA.lines().iterator();
            Iterator<String> iterB = readerB.lines().iterator();
            String lineA = null;
            String lineB = null;
            while (iterA.hasNext() && iterB.hasNext()) {
                if (lineA == null) {
                    lineA = iterA.next();
                }
                if (lineB == null) {
                    lineB = iterB.next();
                }
                int cp = CompareUtil.compare(lineA, lineB, comparator);
                if (cp > 0) {
                    if (isAsc) {
                        writeLine(writer, lineB, true);
                        lineB = null;
                    } else {
                        writeLine(writer, lineA, true);
                        lineA = null;
                    }
                } else if (cp < 0) {
                    if (isAsc) {
                        writeLine(writer, lineA, true);
                        lineA = null;
                    } else {
                        writeLine(writer, lineB, true);
                        lineB = null;
                    }
                } else {
                    writeLine(writer, lineA, true);
                    writeLine(writer, lineB, true);
                    lineA = null;
                    lineB = null;
                }
            }
            //剩余数据合并
            writeLine(writer, lineA, true);
            writeLine(writer, lineB, true);
            while (iterA.hasNext()) {
                writeLine(writer, iterA.next(), true);
            }
            while (iterB.hasNext()) {
                writeLine(writer, iterB.next(), true);
            }
            IoUtil.close(writer);
            //临时文件改名
            FileUtil.move(FileUtil.file(tmpFile), FileUtil.file(destFile), true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(readerA);
            IoUtil.close(readerB);
            IoUtil.close(writer);
            FileUtil.del(tmpFile);
        }
        long stop = System.currentTimeMillis();
        System.out.println(StrUtil.format("文件合并完成,耗时: {} s", (stop - start) / 1000d));
    }

    /**
     * 写一行数据
     *
     * @param writer      writer
     * @param line        一行数据
     * @param ignoreBlank 是否忽略空行
     */
    public static void writeLine(BufferedWriter writer, String line, boolean ignoreBlank) {
        Validate.notNull(writer);
        String writeLine = line;
        if (!StringUtils.endsWith(line, System.lineSeparator())) {
            writeLine += System.lineSeparator();
        }
        if (!ignoreBlank || !StringUtils.isBlank(line)) {
            try {
                writer.write(writeLine);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
