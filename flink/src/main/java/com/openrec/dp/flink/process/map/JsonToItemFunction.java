package com.openrec.dp.flink.process.map;

import com.openrec.dp.flink.util.JsonUtil;
import com.openrec.proto.model.Item;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class JsonToItemFunction implements MapFunction<String, Item> {

    @Override
    public Item map(String s) throws Exception {
        Item item = null;
        if(!StringUtils.isEmpty(s)) {
            item = JsonUtil.jsonToObj(s, Item.class);
        }
        return item;
    }
}
