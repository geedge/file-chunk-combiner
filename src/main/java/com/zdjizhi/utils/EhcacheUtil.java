package com.zdjizhi.utils;

import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.xml.XmlConfiguration;

import java.net.URL;

public class EhcacheUtil {
    private static EhcacheUtil ehcacheUtil = null;
    private static CacheManager cacheManager = null;

    private EhcacheUtil() {
        URL xmlConfigUrl = this.getClass().getClassLoader().getResource("ehcache.xml");
        XmlConfiguration xmlConfiguration = new XmlConfiguration(xmlConfigUrl);
        cacheManager = CacheManagerBuilder.newCacheManager(xmlConfiguration);
        cacheManager.init();
    }

    public static synchronized EhcacheUtil getInstance() {
        if (null == ehcacheUtil) {
            ehcacheUtil = new EhcacheUtil();
        }
        return ehcacheUtil;
    }

    public CacheManager getEhcacheManager() {
        return cacheManager;
    }

    public void close() {
        cacheManager.close();
        ehcacheUtil = null;
    }
}
