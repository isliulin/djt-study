package com.djt.utils;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.metadata.MetadataSerializer;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;

/**
 * Checkpoint 工具
 *
 * @author 　djt317@qq.com
 * @since 　 2022-01-12
 */
public class CheckpointUtils {

    @Test
    public void testCp1() {
        String path = "C:\\Users\\duanjiatao\\Desktop\\rocksdb\\_metadata_uat-120";
        parseCheckpoint(path);
    }

    /**
     * 解析Checkpoint的_metadata文件
     *
     * @param path 文件路径
     */
    public static void parseCheckpoint(String path) {
        BufferedInputStream bis = FileUtil.getInputStream(path);
        DataInputStream dis = new DataInputStream(bis);

        // 通过 Flink 的 Checkpoints 类解析元数据文件
        CheckpointMetadata metadata;
        try {
            metadata = Checkpoints.loadCheckpointMetadata(dis, MetadataSerializer.class.getClassLoader(), path);
        } catch (IOException e) {
            throw new RuntimeException("文件解析失败！", e);
        }
        // 打印当前的 CheckpointId
        System.out.println("CheckpointId: " + metadata.getCheckpointId());

        // 遍历 OperatorState，这里的每个 OperatorState 对应一个 Flink 任务的 Operator 算子
        // 不要与 OperatorState  和 KeyedState 混淆，不是一个层级的概念
        //System.out.println("metadata.getOperatorStates.size=" + metadata.getOperatorStates().size());
        for (OperatorState operatorState : metadata.getOperatorStates()) {
            System.out.println(operatorState);
            // 当前算子的状态大小为 0 ，表示算子不带状态，直接退出
            //if (operatorState.getStateSize() == 0) {
            //    continue;
            //}

            // 遍历当前算子的所有 subtask

            // System.out.println("operatorState.getStates.size=" + operatorState.getStates().size());
            for (OperatorSubtaskState operatorSubtaskState : operatorState.getStates()) {
                // 解析 operatorSubtaskState 的 ManagedKeyedState
                parseManagedKeyedState(operatorSubtaskState);
                // 解析 operatorSubtaskState 的 ManagedOperatorState
                parseManagedOperatorState(operatorSubtaskState);
            }
        }
    }


    /**
     * 解析 operatorSubtaskState 的 ManagedKeyedState
     *
     * @param operatorSubtaskState operatorSubtaskState
     */
    private static void parseManagedKeyedState(OperatorSubtaskState operatorSubtaskState) {
        //System.out.println("ManagedKeyedState.size=" + operatorSubtaskState.getManagedKeyedState().size());
        DecimalFormat df = new DecimalFormat("#######0.00");
        // 遍历当前 subtask 的 KeyedState
        for (KeyedStateHandle keyedStateHandle : operatorSubtaskState.getManagedKeyedState()) {
            // 本案例针对 Flink RocksDB 的增量 Checkpoint 引发的问题，
            // 因此仅处理 IncrementalRemoteKeyedStateHandle
            if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
                // 获取 RocksDB 的 sharedState

                IncrementalRemoteKeyedStateHandle incHandle = (IncrementalRemoteKeyedStateHandle) keyedStateHandle;
                System.out.println(StrUtil.format("CheckpointId= {} " +
                                "BackendIdentifier={} " +
                                "KeyGroupRange={} " +
                                "StateSize={} " +
                                "SharedStateNums={}",
                        incHandle.getCheckpointId(),
                        incHandle.getBackendIdentifier(),
                        incHandle.getKeyGroupRange(),
                        incHandle.getStateSize() / 1024 / 1024,
                        incHandle.getSharedState().size()
                ));
                Map<StateHandleID, StreamStateHandle> sharedState = incHandle.getSharedState();
                // 遍历所有的 sst 文件，key 为 sst 文件名，value 为对应的 hdfs 文件 Handle
                for (Map.Entry<StateHandleID, StreamStateHandle> entry : sharedState.entrySet()) {
                    String name = entry.getKey().getKeyString();
                    StreamStateHandle handle = entry.getValue();
                    String handleName = handle.getClass().getSimpleName();
                    String prefix = StringUtils.rightPad(handleName, 25, "=") + ">";
                    String info = prefix + " sstable文件名: {} 文件大小: {} KB 文件位置: {}";
                    if (handle instanceof FileStateHandle) {
                        FileStateHandle fsHandle = (FileStateHandle) handle;
                        long stateSize = fsHandle.getStateSize();
                        Path filePath = fsHandle.getFilePath();
                        // 打印sst文件相关信息
                        System.out.println(StrUtil.format(info, name, df.format((double) stateSize / 1024),
                                filePath.getPath()));
                    } else if (handle instanceof ByteStreamStateHandle) {
                        ByteStreamStateHandle byteHandle = (ByteStreamStateHandle) handle;
                        long stateSize = byteHandle.getStateSize();
                        System.out.println(StrUtil.format(info, name, df.format((double) stateSize / 1024),
                                StringUtils.substring(byteHandle.getHandleName(), 20)));
                    }
                }
            }
        }
    }


    /**
     * 解析 operatorSubtaskState 的 ManagedOperatorState
     * 注：OperatorState 不支持 Flink 的 增量 Checkpoint，因此本案例可以不解析
     *
     * @param operatorSubState operatorSubtaskState
     */
    private static void parseManagedOperatorState(OperatorSubtaskState operatorSubState) {
        // 遍历当前 subtask 的 OperatorState
        for (OperatorStateHandle operatorStateHandle : operatorSubState.getManagedOperatorState()) {
            StreamStateHandle delegateState = operatorStateHandle.getDelegateStateHandle();
            if (delegateState instanceof FileStateHandle) {
                Path filePath = ((FileStateHandle) delegateState).getFilePath();
                System.out.println(filePath.getPath());
            }
        }
    }


}
