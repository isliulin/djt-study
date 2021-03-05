-- test.student definition

CREATE TABLE `student` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `name` varchar(50) NOT NULL COMMENT '姓名',
  `sex` char(1) NOT NULL COMMENT '性别 1-男 2-女',
  `age` tinyint(3) unsigned NOT NULL COMMENT '年龄',
  `is_del` char(1) NOT NULL DEFAULT '0' COMMENT '删除状态: 0未删除，1删除',
  `remark` text COMMENT '备注',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `student_name_IDX` (`name`) USING BTREE,
  KEY `student_create_time_IDX` (`create_time`) USING BTREE,
  KEY `student_update_time_IDX` (`update_time`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO test.student (name, sex, age, is_del, remark, create_time, update_time)
VALUES(?, ?, ?, ?, ?, ?, ?);
