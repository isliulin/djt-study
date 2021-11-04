package com.djt.test.service;

import com.djt.service.impl.RiskRuleService;
import org.junit.Test;

import java.util.Set;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-10-28
 */
public class RiskRuleServiceTest {

    private final RiskRuleService riskRuleService = new RiskRuleService();

    @Test
    public void testGetRuleCode() {
        Set<String> ruleCodeSet = riskRuleService.getRuleCode("商户当日借记卡单笔成功交易金额>=5000");
        System.out.println(ruleCodeSet);
    }

}
