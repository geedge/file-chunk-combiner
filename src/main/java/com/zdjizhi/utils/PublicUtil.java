package com.zdjizhi.utils;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.URLUtil;
import cn.hutool.crypto.digest.DigestUtil;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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

    public static boolean checkHealth(String endpoint) {
        boolean isHealth = false;
        InputStream inputStream = null;
        try {
            URL url = new URL(URLUtil.normalize(endpoint + "/actuator/health"));
            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod("GET");
            urlConnection.setConnectTimeout(10000);
            urlConnection.setReadTimeout(10000);
            int responseCode = urlConnection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                inputStream = urlConnection.getInputStream();
                String responseBody = IoUtil.read(inputStream, StandardCharsets.UTF_8);
                if (responseBody.contains("UP")) {
                    isHealth = true;
                }
            }
        } catch (Exception ignored) {
        } finally {
            IoUtil.close(inputStream);
        }
        return isHealth;
    }
}
