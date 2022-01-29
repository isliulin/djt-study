package com.djt.tools.test;

import com.djt.tools.AbsTools;
import com.djt.tools.impl.ConsumeData;
import com.djt.tools.impl.HdfsTools;
import com.djt.tools.impl.MakeData;
import com.djt.tools.impl.OrcParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.djt.tools.AbsTools.PROPS;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-04-09
 */
public class ToolsTest {

    private static final Logger log = LogManager.getLogger(ToolsTest.class);

    @Before
    public void before() {
    }

    @After
    public void after() {
    }

    @Test
    public void testHdfsTools() {
        AbsTools tools = new HdfsTools();
        tools.execute(null);
    }

    @Test
    public void testMakeData() {
        AbsTools tools = new MakeData();
        tools.execute(null);
    }

    @Test
    public void testConsumeData() {
        PROPS.setProperty("kafka.topic.event", "RISK_ANALYSIS_EVENT");
        PROPS.setProperty("group.id", "GROUP_RISK_ANALYSIS_STAT");
        AbsTools tools = new ConsumeData();
        tools.execute(null);
    }

    @Test
    public void testOrcParser() {
        AbsTools tools = new OrcParser();
        tools.execute(null);
    }
}
