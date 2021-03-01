package com.djt.test.spark.action

import com.djt.spark.action.impl.FirstSparkAction
import org.junit.Test

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-03
 */
class SparkActionTest extends AbsActionTest {

    @Test
    def testConfig(): Unit = {

    }

    @Test
    def testFirstAction(): Unit = {
        new FirstSparkAction(config).action()
    }

}
