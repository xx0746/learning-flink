package com.dajiangtai.stream.userPurchaseBehaviorTracker.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * 配置流
 *
 * @author dajiangtai
 * @create 2019-06-24-13:06
 */
@Data
@ToString
public class Config implements Serializable {
    private static final long serialVersionUID = 2175805295049245714L;
    private String channel;
    private String registerDate;
    private Integer historyPurchaseTimes;
    private Integer maxPurchasePathLength;

    public Config(){}

    public Config(String channel,String registerDate,Integer historyPurchaseTimes,Integer maxPurchasePathLength){
        this.channel = channel;
        this.registerDate = registerDate;
        this.historyPurchaseTimes = historyPurchaseTimes;
        this.maxPurchasePathLength = maxPurchasePathLength;
    }
}
