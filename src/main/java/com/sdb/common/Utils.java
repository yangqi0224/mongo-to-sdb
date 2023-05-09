package com.sdb.common;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName Utils
 * @Description 请描述类的业务用途
 * @Author yangqi
 * @Date 2023/3/17 20:32
 * @Version 1.0
 **/
public class Utils {
    public static List<String> stringToList(String str,String split){
        List<String> list = new ArrayList<>();
        String[] split1 = str.split(split);
        for (String s:split1){
            list.add(s);
        }
        return list;
    }
}
