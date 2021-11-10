package com.djt.test.service;

import cn.hutool.core.util.StrUtil;
import com.djt.service.impl.OrcToOracleService;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-11-08
 */
public class OrcToOracleServiceTest {

    private final OrcToOracleService service = new OrcToOracleService();

    private long start = 0L;

    @Before
    public void before() {
        System.out.println("程序开始运行...");
        start = System.currentTimeMillis();
    }

    @After
    public void after() {
        long stop = System.currentTimeMillis();
        System.out.println(StrUtil.format("运行完成...耗时：{} s", (stop - start) / 1000));
    }

    @Test
    public void test1() {
        String file = "C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\orc-001";
        String[] dateList = {"2022-01-01", "2022-01-02"};
        for (String date : dateList) {
            String suffix = StringUtils.substring(date, -5);
            String table = "order_user.t_pay_order_" + suffix.replace("-", "");
            service.readOrcToOracle(file, date, table);
        }
    }
}
