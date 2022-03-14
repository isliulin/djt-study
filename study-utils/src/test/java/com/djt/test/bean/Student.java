package com.djt.test.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * @author 　djt317@qq.com
 * @since 　 2022-03-04
 */
@Data
public class Student implements Serializable {

    private String id;

    private String name;

}
