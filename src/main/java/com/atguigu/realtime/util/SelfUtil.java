package com.atguigu.realtime.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SelfUtil {

    public static String timestampToDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts));
    }

    public static <T> List<T> iterableToList(Iterable<T> elements) {
        List<T> list = new ArrayList<>();
        elements.forEach(list::add);
        return list;
    }
}
