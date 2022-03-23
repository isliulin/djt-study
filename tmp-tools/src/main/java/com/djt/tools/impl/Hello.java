package com.djt.tools.impl;

import com.djt.tools.AbsTools;
import lombok.extern.log4j.Log4j2;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-06-25
 */
@Log4j2
public class Hello extends AbsTools {

    @Override
    public void doExecute(String[] args) {
        log.info("Hello World.");
    }
}
