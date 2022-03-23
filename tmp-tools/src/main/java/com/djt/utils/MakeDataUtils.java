package com.djt.utils;

import cn.binarywang.tools.generator.ChineseMobileNumberGenerator;
import cn.binarywang.tools.generator.ChineseNameGenerator;
import cn.binarywang.tools.generator.bank.BankCardNumberGenerator;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.thread.ThreadUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;

import java.io.BufferedReader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;


/**
 * 造数据工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-04
 */
@Log4j2
public class MakeDataUtils {

    /**
     * 随机数生成器
     */
    public static final Random RANDOM = new Random();

    /**
     * 业务大类
     */
    public static final String[] BUSI_TYPE_ARR = {"1001", "2001"};

    /**
     * 支付方式
     */
    public static final String[] PAY_TYPE_ARR = {"wxpay", "alipay", "unionpay", "bankpay"};

    /**
     * 交易状态
     */
    public static final String[] STATUS = {"2", "2", "2", "2", "2", "2", "2", "2", "2", "3"};

    /**
     * 商户号
     */
    public static String[] MER_NO_ARR;

    /**
     * 姓名
     */
    public static String[] NAME_ARR;

    /**
     * 返回码
     */
    public static final String[] RET_CODE_ARR =
            {"00", "00", "00", "00", "00", "00", "00", "00", "01", "VZ"};

    /**
     * 手续费类型
     */
    public static final String[] FEE_TYPE_ARR =
            {"01", "11", "02", "12"};


    /**
     * 银行卡号
     */
    public static String[] CARD_NO_ARR;

    static {
        updateBaseInfo();
    }

    /**
     * 更新基础信息
     */
    public static void updateBaseInfo() {
        //生成商户信息
        int merchNum = 300000;
        MER_NO_ARR = new String[merchNum];
        NAME_ARR = new String[merchNum];
        for (int i = 0; i < merchNum; i++) {
            MER_NO_ARR[i] = ChineseMobileNumberGenerator.getInstance().generate();
            NAME_ARR[i] = ChineseNameGenerator.getInstance().generateOdd();
        }

        //生成卡信息
        int cardNum = 500000;
        CARD_NO_ARR = new String[cardNum];
        for (int i = 0; i < cardNum; i++) {
            CARD_NO_ARR[i] = BankCardNumberGenerator.getInstance().generate();
        }
    }

    /**
     * 起始时间
     * LocalDateTime.now();
     */
    public static LocalDateTime TIME_START =
            LocalDateTime.parse("1970-01-01 00:00:00", DatePattern.NORM_DATETIME_FORMATTER);

    private static int dayOld = Integer.parseInt(TIME_START.format(DatePattern.PURE_DATE_FORMATTER));

    /**
     * 生成订单流水
     *
     * @param offset 时间偏移量
     * @return Event
     */
    public static JSONObject makePayOrderEvent(long offset, int interval) {
        LocalDateTime thisTime = TIME_START.plus(offset * interval, ChronoUnit.MILLIS);
        int dayNew = Integer.parseInt(thisTime.format(DatePattern.PURE_DATE_FORMATTER));
        if (dayNew > dayOld) {
            updateBaseInfo();
            dayOld = dayNew;
        }
        String timeStr = thisTime.format(DatePattern.NORM_DATETIME_FORMATTER);

        int merIndex = RANDOM.nextInt(MER_NO_ARR.length);
        String merNo = MER_NO_ARR[merIndex];
        String merName = NAME_ARR[merIndex];

        int cardIndex = merIndex + RANDOM.nextInt(3);
        String cardNo = CARD_NO_ARR[cardIndex];

        JSONObject eventJson = new JSONObject();
        eventJson.put("type", "02");
        eventJson.put("subject", "test");
        eventJson.put("timestamp", System.currentTimeMillis());
        eventJson.put("event_id", UUID.randomUUID().toString(true));
        JSONObject event = new JSONObject();
        event.put("order_id", StringUtils.leftPad(String.valueOf(offset), 20, "0"));
        event.put("ori_order_id", "");
        event.put("busi_type", BUSI_TYPE_ARR[RANDOM.nextInt(BUSI_TYPE_ARR.length)]);
        event.put("out_order_id", "");
        event.put("merch_no", merNo);
        event.put("term_no", merNo);
        event.put("term_sn", merNo);
        event.put("print_merch_name", merName);
        event.put("agent_id", "123");
        event.put("sources", "pos+/posp_api");
        event.put("trans_time", timeStr);
        event.put("amount", Math.abs(RANDOM.nextInt(100 * 10000 * 100) * 100));
        event.put("status", STATUS[RANDOM.nextInt(STATUS.length)]);
        event.put("expire_time", timeStr);
        event.put("trans_type", "SALE");
        event.put("pay_type", PAY_TYPE_ARR[RANDOM.nextInt(PAY_TYPE_ARR.length)]);
        event.put("area_code", "440305");
        event.put("location", "192.168.10.6");
        event.put("fee", "1");
        event.put("fee_type", FEE_TYPE_ARR[RANDOM.nextInt(FEE_TYPE_ARR.length)]);
        event.put("pay_token", cardNo);
        event.put("ret_code", RET_CODE_ARR[RANDOM.nextInt(RET_CODE_ARR.length)]);
        event.put("ret_msg", "测试");
        event.put("auth_code", "132456");
        event.put("remark", "备注" + offset);
        event.put("create_time", timeStr);
        event.put("update_time", timeStr);

        eventJson.put("event", event);
        return eventJson;
    }

    /**
     * 往kafka中造数据
     *
     * @param topic      主题
     * @param size       条数
     * @param sleep      休眠时间
     * @param startTime  起始时间
     * @param interval   数据之间时间间隔
     * @param properties 配置
     */
    public static void makeDataToKafka(String topic, long size, long sleep,
                                       LocalDateTime startTime, int interval, Properties properties) {
        Producer<String, String> producer = KafkaUtils.createProducer(properties);
        TIME_START = startTime;
        for (long i = 0; i < size; i++) {
            JSONObject event = makePayOrderEvent(i, interval);
            KafkaUtils.sendMessage(producer, topic, event.getString("event_id"), JSON.toJSONString(event), false);
            ThreadUtil.sleep(sleep);
        }
    }

    /**
     * 读取orc文件发送至kafka
     *
     * @param filePath   文件路径
     * @param starDate   起始日期
     * @param sleep      休眠时间
     * @param topic      主题
     * @param properties 配置
     * @param isPrint    是否打印数据
     */
    public static void readFileToKafka(String filePath, LocalDate starDate, long sleep,
                                       String topic, Properties properties, boolean isPrint) {
        String dateStr = starDate.format(DatePattern.NORM_DATE_FORMATTER);
        BufferedReader reader = null;
        Producer<String, String> producer = null;
        try {
            reader = FileUtil.getUtf8Reader(filePath);
            producer = KafkaUtils.createProducer(properties);
            Iterator<String> iter = reader.lines().iterator();
            while (iter.hasNext()) {
                String line = iter.next();
                JSONObject event = JSON.parseObject(line);
                for (String key : event.keySet()) {
                    Object value = event.get(key);
                    value = value == null ? null : value.toString();
                    if (key.endsWith("_time") && value != null) {
                        value = dateStr + value.toString().substring(10);
                    }
                    event.put(key, value);
                }
                String uid = UUID.randomUUID().toString(true);
                JSONObject msgJson = new JSONObject();
                msgJson.put("type", "02");
                msgJson.put("subject", "test");
                msgJson.put("timestamp", System.currentTimeMillis());
                msgJson.put("event_id", uid);
                msgJson.put("event", event);
                KafkaUtils.sendMessage(producer, topic, uid, msgJson.toJSONString(), isPrint);
                ThreadUtil.sleep(sleep);
            }
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(reader);
            IoUtil.close(producer);
        }
    }

}
