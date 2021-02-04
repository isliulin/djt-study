package com.djt.test.spark.action

import com.djt.spark.action.impl.FirstSparkAction
import org.junit.Test

/**
 * @author 　djt317@qq.com
 * @date 　  2021-02-03 10:25
 */
class SparkActionTest extends AbsActionTest {

    @Test
    def testFirstAction(): Unit = {
        new FirstSparkAction(config).action()
    }

}
