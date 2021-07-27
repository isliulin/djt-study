package com.djt.test.bean;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * 订单流水
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-27
 */
@Data
public class PayOrder {

    /**
     * 订单号
     */
    @JSONField(name = "ORDER_ID")
    private String orderId;

    /**
     * 原订单号
     */
    @JSONField(name = "ORI_ORDER_ID")
    private String oriOrderId;

    /**
     * 业务大类
     */
    @JSONField(name = "BUSI_TYPE")
    private String busiType;

    /**
     * 外部订单号
     */
    @JSONField(name = "OUT_ORDER_ID")
    private String outOrderId;

    /**
     * 商户号
     */
    @JSONField(name = "MERCH_NO")
    private String merchNo;

    /**
     * 终端号
     */
    @JSONField(name = "TERM_NO")
    private String termNo;

    /**
     * 机身号
     */
    @JSONField(name = "PHY_NO")
    private String phyNo;

    /**
     * 打印商户名
     */
    @JSONField(name = "PRINT_MERCH_NAME")
    private String printMerchName;

    /**
     * 机构号/代理商号
     */
    @JSONField(name = "AGENT_ID")
    private String agentId;

    /**
     * 服务请求来源
     */
    @JSONField(name = "SOURCES")
    private String sources;

    /**
     * 交易时间
     */
    @JSONField(name = "TRANS_TIME")
    private String transTime;

    /**
     * 订单金额
     */
    @JSONField(name = "AMOUNT")
    private String amount;

    /**
     * 订单状态;0-初始 1-待确认 2-成功 3-失败
     */
    @JSONField(name = "STATUS")
    private String status;

    /**
     * 支付过期时间
     */
    @JSONField(name = "EXPIRE_TIME")
    private String expireTime;

    /**
     * 交易类型：SALE(消费),VSALE(消费撤销),REFUND(退货),AUTH(预授权),VAUTH(预授权撤销),
     * CAUTH(预授权完成),VCAUTH(预授权完成撤销),RCAUTH(预授权完成退款)
     */
    @JSONField(name = "TRANS_TYPE")
    private String transType;

    /**
     * 支付方式,微信支付-wxpay;支付宝-alipay;银联二维码-unionpay;银行卡-bankpay
     */
    @JSONField(name = "PAY_TYPE")
    private String payType;

    /**
     * 交易地区码
     */
    @JSONField(name = "AREA_CODE")
    private String areaCode;

    /**
     * 交易位置
     */
    @JSONField(name = "LOCATION")
    private String location;

    /**
     * 手续费，正常扣除手续费使用填写使用+号，退回手续费请使用-号
     */
    @JSONField(name = "FEE")
    private String fee;

    /**
     * 手续费类型(同计费系统)
     */
    @JSONField(name = "FEE_TYPE")
    private String feeType;

    /**
     * 提交支付所使用的tocken,如果是是用微信支付宝，银联二维码，则是使用的auth_code,如果是使用的是刷卡支付，则填写脱敏后的银行卡号
     */
    @JSONField(name = "PAY_TOKEN")
    private String payToken;

    /**
     * 响应码
     */
    @JSONField(name = "RET_CODE")
    private String retCode;

    /**
     * 响应信息
     */
    @JSONField(name = "RET_MSG")
    private String retMsg;

    /**
     * 授权码
     */
    @JSONField(name = "AUTH_CODE")
    private String authCode;

    /**
     * 备注
     */
    @JSONField(name = "REMARK")
    private String remark;

    /**
     * 创建时间
     */
    @JSONField(name = "CREATE_TIME")
    private String createTime;

    /**
     * 更新时间
     */
    @JSONField(name = "UPDATE_TIME")
    private String updateTime;

    /**
     * 事件时间(交易时间)
     */
    @JSONField(name = "EVENT_TIME")
    private long eventTime;

    public void setTransTime(String transTime) {
        this.transTime = transTime;
        this.eventTime = LocalDateTimeUtil.toEpochMilli(LocalDateTimeUtil.parse(transTime, DatePattern.NORM_DATETIME_FORMATTER));
    }

}
