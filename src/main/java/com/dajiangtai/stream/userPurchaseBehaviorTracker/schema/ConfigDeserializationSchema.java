package com.dajiangtai.stream.userPurchaseBehaviorTracker.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.dajiangtai.stream.userPurchaseBehaviorTracker.model.Config;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

/**
 * 配置流反序列化类
 *
 * @author dajiangtai
 * @create 2019-06-24-13:10
 */
public class ConfigDeserializationSchema implements KeyedDeserializationSchema<Config> {
    @Override
    public Config deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        return JSON.parseObject(new String(message),new TypeReference<Config>(){});
    }

    @Override
    public boolean isEndOfStream(Config nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Config> getProducedType() {
        return TypeInformation.of(new TypeHint<Config>() {});
    }
}
