package com.djt.test.other

import org.junit.Test

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-24
 */
class ScalaTest {

    @Test
    def testNull(): Unit = {
        println(null.asInstanceOf[Int])
    }

    @Test
    def testIndex(): Unit = {
        val keyList = Array[String]("t_table_1", "t_table_2", "t_table_3", "t_table_3", "t_table_4", "t_table_5")
        println(keyList.indexOf("t_table_1"))
        println(keyList.indexOf("t_table_2"))
        println(keyList.indexOf("t_table_3"))
        println(keyList.indexOf("t_table_4"))
        println(keyList.indexOf("t_table_5"))
    }
}
