package com.openrec.dp.flink.process.map;

import com.openrec.proto.model.User;
import org.apache.flink.api.common.functions.MapFunction;

public class StringToUserFunction implements MapFunction<String, User> {

    @Override
    public User map(String s) throws Exception {
        User user = null;
        String[] ss = s.split(",");
        if (ss.length == 12) {
            user = new User();
            user.setId(ss[0]);
            user.setDeviceId(ss[1]);
            user.setName(ss[2]);
            user.setGender(ss[3]);
            user.setAge(Integer.parseInt(ss[4]));
            user.setCountry(ss[5]);
            user.setCity(ss[6]);
            user.setPhone(ss[7]);
            user.setTags(null);
            user.setRegisterTime(ss[9]);
            user.setLoginTime(ss[10]);
            user.setExtFields(ss[11]);
        }
        return user;
    }
}
