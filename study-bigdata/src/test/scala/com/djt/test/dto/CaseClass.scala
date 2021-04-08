package com.djt.test.dto

/**
 * 样例类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-01
 */
object CaseClass {

    case class Person(id: Int, name: String, age: Int)

    case class TermInfo(dt: String = null,
                        flag: String = null,
                        mer_no: String = null,
                        term_no: String = null,
                        belong_branch: String = null,
                        branch_company: String = null,
                        r_agt_id: String = null,
                        r_agt_name: String = null,
                        product_type: String = null,
                        term_type: String = null,
                        min_trd_date: String = null) extends Serializable


}
