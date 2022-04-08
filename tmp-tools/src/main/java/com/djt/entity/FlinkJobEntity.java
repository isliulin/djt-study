package com.djt.entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.util.Map;

/**
 * Flink任务信息
 *
 * @author 　djt317@qq.com
 * @since 　 2022-04-07
 */
@Data
public class FlinkJobEntity {

    @JSONField(name = "jid")
    private String jid;

    @JSONField(name = "name")
    private String name;

    @JSONField(name = "state")
    private String state;

    @JSONField(name = "vertices")
    private Map<String, Vertex> vertices;

    @JSONField(name = "taskmanagers")
    private Map<String, TaskManager> taskmanagers;

    @Data
    public static class Vertex {

        @JSONField(name = "id")
        private String id;

        @JSONField(name = "name")
        private String name;

        @JSONField(name = "parallelism")
        private int parallelism;

        @JSONField(name = "status")
        private String status;
    }

    @Data
    public static class TaskManager {

        @JSONField(name = "id")
        private String id;
    }

}
