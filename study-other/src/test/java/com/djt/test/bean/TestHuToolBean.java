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
public class TestHuToolBean {

    private Integer id;

    //Hutool中可以下划线，也可以驼峰，自动识别
    private Date createTime;

    private Date modify_time;

    private String content;

}
