package com.djt.utils;

import cn.binarywang.tools.generator.ChineseMobileNumberGenerator;
import cn.binarywang.tools.generator.ChineseNameGenerator;
import cn.binarywang.tools.generator.bank.BankCardNumberGenerator;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.thread.ThreadUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.clients.producer.Producer;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
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
            {"00", "01"};

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
        //生成商户信息
        int merchNum = 500000;
        MER_NO_ARR = new String[merchNum];
        NAME_ARR = new String[merchNum];
        for (int i = 0; i < merchNum; i++) {
            MER_NO_ARR[i] = ChineseMobileNumberGenerator.getInstance().generate();
            NAME_ARR[i] = ChineseNameGenerator.getInstance().generateOdd();
        }

        //生成卡信息
        int cardNum = 1000000;
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

    /**
     * 生成订单流水
     *
     * @param offset 时间偏移量
     * @return Event
     */
    public static JSONObject makePayOrderEvent(long offset, int interval) {
        LocalDateTime thisTime = TIME_START.plus(offset * interval, ChronoUnit.MILLIS);
        int merIndex = RANDOM.nextInt(MER_NO_ARR.length);
        String merNo = MER_NO_ARR[merIndex];
        String merName = NAME_ARR[merIndex];

        int cardIndex = merIndex + RANDOM.nextInt(5);
        String cardNo = CARD_NO_ARR[cardIndex];

        JSONObject eventJson = new JSONObject();
        eventJson.put("type", "02");
        eventJson.put("subject", "test");
        eventJson.put("timestamp", System.currentTimeMillis());
        eventJson.put("event_id", UUID.randomUUID().toString(true));
        JSONObject event = new JSONObject();
        event.put("order_id", System.currentTimeMillis());
        event.put("ori_order_id", "");
        event.put("busi_type", BUSI_TYPE_ARR[RANDOM.nextInt(BUSI_TYPE_ARR.length)]);
        event.put("out_order_id", "");
        event.put("merch_no", merNo);
        event.put("term_no", merNo);
        event.put("term_sn", merNo);
        event.put("print_merch_name", merName);
        event.put("agent_id", "123");
        event.put("sources", "pos+/posp_api");
        event.put("trans_time", thisTime.format(DatePattern.NORM_DATETIME_FORMATTER));
        event.put("amount", Math.abs(RANDOM.nextInt(30 * 10000 * 100) * 100));
        event.put("status", "2");
        event.put("expire_time", thisTime.plusSeconds(1).format(DatePattern.NORM_DATETIME_FORMATTER));
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
        event.put("create_time", thisTime.format(DatePattern.NORM_DATETIME_FORMATTER));
        event.put("update_time", thisTime.format(DatePattern.NORM_DATETIME_FORMATTER));

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
            KafkaUtils.sendMessage(producer, topic, event.getString("event_id"), JSON.toJSONString(event));
            ThreadUtil.sleep(sleep);
        }
    }

    /**
     * 读取orc文件发送至kafka
     *
     * @param topic      主题
     * @param sleep      休眠时间
     * @param filePath   文件路径
     * @param startTime  起始日期
     * @param interval   时间间隔
     * @param properties 配置
     */
    public static void readOrcToKafka(String filePath, LocalDateTime startTime, long interval,
                                      String topic, long sleep, Properties properties) {
        TIME_START = startTime;
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        Reader reader = null;
        RecordReader rows = null;
        try {
            reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
            rows = reader.rows();
            TypeDescription schema = reader.getSchema();
            List<TypeDescription> children = schema.getChildren();
            int batchSize = 10000;
            VectorizedRowBatch batch = schema.createRowBatch(batchSize);
            int numberOfChildren = children.size();
            List<OrcStruct> resultList = new ArrayList<>();
            Producer<String, String> producer = KafkaUtils.createProducer(properties);
            while (rows.nextBatch(batch)) {
                for (int r = 0; r < batch.size; r++) {
                    OrcStruct result = new OrcStruct(schema);
                    for (int i = 0; i < numberOfChildren; ++i) {
                        result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], 1,
                                children.get(i), result.getFieldValue(i)));
                    }
                    resultList.add(result);
                }
                sendToKafka(resultList, interval, topic, sleep, producer);
                resultList.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(rows);
            IoUtil.close(reader);
        }
        log.info("数据生产完成");
    }

    private static long count = 0L;

    /**
     * 发送orc数据到kafka
     *
     * @param resultList 结果列表
     * @param interval   时间间隔
     * @param topic      主题
     * @param sleep      休眠时间
     * @param producer   生产者
     */
    public static void sendToKafka(List<OrcStruct> resultList, long interval,
                                   String topic, long sleep, Producer<String, String> producer) {
        if (CollectionUtils.isEmpty(resultList)) {
            return;
        }
        for (OrcStruct struct : resultList) {
            List<String> names = struct.getSchema().getFieldNames();
            JSONObject eventJson = new JSONObject();
            eventJson.put("type", "02");
            eventJson.put("subject", "test");
            eventJson.put("timestamp", System.currentTimeMillis());
            eventJson.put("event_id", UUID.randomUUID().toString(true));
            JSONObject event = new JSONObject();
            String time = TIME_START.plus(++count * interval, ChronoUnit.MILLIS).format(DatePattern.NORM_DATETIME_FORMATTER);
            for (int i = 0; i < names.size(); i++) {
                String name = names.get(i);
                if ("term_sn".equalsIgnoreCase(name)) {
                    name = "phy_no";
                }
                String value = String.valueOf(struct.getFieldValue(i));
                if (name.endsWith("_time")) {
                    value = time;
                }
                event.put(name, value);
            }
            eventJson.put("event", event);
            KafkaUtils.sendMessage(producer, topic, eventJson.getString("event_id"), JSON.toJSONString(eventJson));
            ThreadUtil.sleep(sleep);
        }
    }


}
