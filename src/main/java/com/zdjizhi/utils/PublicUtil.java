package com.zdjizhi.utils;

import cn.hutool.crypto.digest.DigestUtil;

import java.util.*;

public class PublicUtil {

    public static String getUUID() {
        return UUID.randomUUID().toString().replace("-", "").toLowerCase();
    }

    public static String getRowKey(String filename) {
        String md5str = DigestUtil.md5Hex(filename);
        md5str = md5str.substring(8, 24);
        return md5str;
    }

    public static String getIndexDataHead(String filename) {
        return getRowKey(filename).substring(0, 1);
    }
}
