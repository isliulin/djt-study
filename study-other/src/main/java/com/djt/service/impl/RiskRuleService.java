package com.djt.service.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.djt.dao.impl.OracleDao;
import com.djt.service.AbstractService;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.*;

/**
 * 风控规则抽象类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-10-28
 */
public class RiskRuleService extends AbstractService {

    private final OracleDao dao;

    public RiskRuleService() {
        dao = OracleDao.getInstance(config);
    }

    @Override
    protected void updateConfig(Properties config) {
        config.put("oracle.druid.Url", "jdbc:oracle:thin:@172.20.6.22:1521:mpos");
        config.put("oracle.druid.Username", "riskctrl_user");
        config.put("oracle.druid.Password", "e1c9599010ace999c41700ab183d9fe9");
    }

    public Set<String> getRuleCode(String ruleDesc) {
        Set<String> ruleCodeSet = new HashSet<>();
        String sql = "SELECT rule_id,param,product,\n" +
                "       rule_desc\n" +
                "FROM riskctrl_user.t_rc_rule_pond\n" +
                "WHERE rule_type='03'\n" +
                "  AND rule_desc LIKE '%'||?||'%'";

        String sql2 = "SELECT rule_id,rule_code,product,param,rule_desc\n" +
                "FROM riskctrl_user.t_rc_rule_pond\n" +
                "WHERE rule_type='02'\n" +
                "  AND param LIKE '%\"'||?||'\"%'";
        try {
            List<Map<String, Object>> resultMapList = dao.query(sql, ruleDesc);
            for (Map<String, Object> resultMap : resultMapList) {
                System.out.println("=======================================");
                System.out.println(StrUtil.format("rule_id={} product={} param={} rule_desc={}",
                        resultMap.get("RULE_ID"),resultMap.get("PRODUCT"),resultMap.get("PARAM"), resultMap.get("RULE_DESC")));
                JSONObject paramJson = JSON.parseObject(resultMap.getOrDefault("PARAM", "{}").toString());
                List<String> fieldList = getParams(paramJson);
                for (String field : fieldList) {
                    List<Map<String, Object>> ruleCodeMapList = dao.query(sql2, field);
                    for (Map<String, Object> uleCodeMap : ruleCodeMapList) {
                        System.out.println(StrUtil.format("rule_id={} rule_code={} product={}  param={} rule_desc={}",
                                uleCodeMap.get("RULE_ID"),
                                uleCodeMap.get("RULE_CODE"), uleCodeMap.get("PRODUCT"),
                                uleCodeMap.get("PARAM"), uleCodeMap.get("RULE_DESC")));
                        ruleCodeSet.add(uleCodeMap.getOrDefault("RULE_CODE", "").toString());
                    }
                }
            }

        } catch (SQLException throwables) {
            throw new RuntimeException("查询失败：", throwables);
        }
        return ruleCodeSet;
    }

    private List<String> getParams(JSONObject paramJson) {
        List<String> fieldList = new ArrayList<>();
        for (String field : paramJson.keySet()) {
            fieldList.add(StringUtils.removeEnd(field, "_limit"));
        }
        return fieldList;
    }

}
