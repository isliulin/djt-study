package com.djt.test.bean;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.PrintStream;

/**
 * 控制台输出重定向
 *
 * @author 　djt317@qq.com
 * @since 　 2021-09-09
 */
@Log4j2
public class Log4j2PrintStream {

    public static final Marker marker = MarkerManager.getMarker("sink");


    public static void redirectSystemPrint() {
        PrintStream printStreamOut = createLoggingWrapper(System.out, false);
        System.setOut(printStreamOut);

        PrintStream printStreamErr = createLoggingWrapper(System.err, true);
        System.setErr(printStreamErr);
    }

    private static PrintStream createLoggingWrapper(final PrintStream printStream, final boolean isErr) {
        return new PrintStream(printStream) {

            private void printToLog(Object message) {
                if (isErr) {
                    log.error(marker, "{}", message);
                } else {
                    log.info(marker, "{}", message);
                }
            }

            @Override
            public void print(final String string) {
                printToLog(string);
            }

            @Override
            public void print(boolean b) {
                printToLog(b);
            }

            @Override
            public void print(char c) {
                printToLog(c);
            }

            @Override
            public void print(int i) {
                printToLog(i);
            }

            @Override
            public void print(long l) {
                printToLog(l);
            }

            @Override
            public void print(float f) {
                printToLog(f);
            }

            @Override
            public void print(double d) {
                printToLog(d);
            }

            @Override
            public void print(char @NonNull [] x) {
                printToLog(x);
            }

            @Override
            public void print(Object obj) {
                printToLog(obj);
            }
        };
    }
}
