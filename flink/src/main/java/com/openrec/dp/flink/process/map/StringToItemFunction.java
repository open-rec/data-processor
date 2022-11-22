package com.openrec.dp.flink.process.map;

import com.openrec.proto.model.Item;
import org.apache.flink.api.common.functions.MapFunction;

public class StringToItemFunction implements MapFunction<String, Item> {

    @Override
    public Item map(String s) throws Exception {
        Item item = null;
        String[] ss=s.split(",");
        if(ss.length==11) {
            item = new Item();
            item.setId(ss[0]);
            item.setTitle(ss[1]);
            item.setCategory(ss[2]);
            item.setTags(ss[3]);
            item.setScene(ss[4]);
            item.setPubTime(ss[5]);
            item.setModifyTime(ss[6]);
            item.setExpireTime(ss[7]);
            item.setStatus(Integer.parseInt(ss[8]));
            item.setWeight(Integer.parseInt(ss[9]));
            item.setExtFields(ss[10]);
        }
        return item;
    }
}
