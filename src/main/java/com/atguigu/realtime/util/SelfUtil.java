package com.atguigu.realtime.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class SelfUtil {

    public static String timestampToDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts));
    }
}
