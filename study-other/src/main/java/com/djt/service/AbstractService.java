package com.djt.service;

import com.alibaba.druid.pool.ha.PropertiesUtils;
import com.djt.dao.AbstractDao;

import java.util.Properties;

/**
 * 抽象服务类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-02
 */
public abstract class AbstractService {

    protected Properties config;
    protected AbstractDao dao;

    public AbstractService() {
        config = PropertiesUtils.loadProperties("/config.properties");
        updateConfig(config);
    }

    protected void updateConfig(Properties config) {}
}
