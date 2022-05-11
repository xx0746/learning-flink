package com.dajiangtai.stream.userPurchaseBehaviorTracker.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.dajiangtai.stream.userPurchaseBehaviorTracker.model.UserEvent;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

/**
 * 反序列化
 *
 * @author dajiangtai
 * @create 2019-06-24-10:18
 */
public class UserEventDeserializationSchema implements KeyedDeserializationSchema {
    @Override
    public Object deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        return JSON.parseObject(new String(message),new TypeReference<UserEvent>(){});
    }

    @Override
    public boolean isEndOfStream(Object nextElement) {
        return false;
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(new TypeHint<UserEvent>() {
        });
    }
}
