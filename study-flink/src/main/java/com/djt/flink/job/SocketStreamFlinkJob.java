package com.djt.flink.job;

import cn.hutool.setting.Setting;
import com.djt.flink.utils.ConfigConstants;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 监听socket流 统计单词计数
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-14
 */
@Log4j2
public class SocketStreamFlinkJob extends AbsFlinkJob {

    public SocketStreamFlinkJob(String jobName, Setting setting) {
        super(jobName, setting);
    }

    @Override
    public void executeAction() {
        String host = setting.get("socket", ConfigConstants.Socket.FLINK_SOCKET_STREAM_HOST);
        int port = Integer.parseInt(setting.get("socket", ConfigConstants.Socket.FLINK_SOCKET_STREAM_PORT));
        String delimiter = setting.get("socket", ConfigConstants.Socket.FLINK_SOCKET_STREAM_DELIMITER);
        DataStreamSource<String> streamSource = exeEnv.socketTextStream(host, port, delimiter);
        DataStream<Tuple2<String, Long>> wordCount = streamSource.flatMap((FlatMapFunction<String, Tuple2<String, Long>>)
                (line, collector) -> {
                    String[] words = line.split("\\s");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1L));
                    }
                }).returns(new TypeHint<Tuple2<String, Long>>() {
        })
        .keyBy(tuple2 -> tuple2.getField(0)).window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1))).sum("f1");
        wordCount.print();
    }
}
