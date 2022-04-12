package com.djt.test.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.io.Serializable;

/**
 * @author 　djt317@qq.com
 * @since 　 2022-03-04
 */
@Data
public class Student implements Serializable {

    @JSONField(name = "id")
    private String id;

    private String name;

    private String birthPlace;

    @JSONField(name = "birth_place")
    public String getBirthPlace() {
        return birthPlace;
    }

    @JSONField(name = "key")
    public void setId(String id) {
        this.id = id;
    }
}
