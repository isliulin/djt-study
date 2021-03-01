package com.djt.test.bean;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-01
 */
@Getter
@Setter
@ToString
public class TestDbUtilsBean {

    private Integer id;

    //注意：dbutils中字段名要与表字段名保持一致，大小写无所谓，否则映射不到
    private Date create_time;

    private Date modify_time;

    private String content;

}
