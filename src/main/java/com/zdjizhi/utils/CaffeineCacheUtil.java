package com.zdjizhi.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.zdjizhi.config.Configs;
import com.zdjizhi.pojo.FileChunk;
import org.apache.flink.configuration.Configuration;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.TimeUnit;

public class CaffeineCacheUtil {
    private static CaffeineCacheUtil caffeineCacheUtil = null;
    private static Cache<String, FileChunk> caffeineCache = null;

    private CaffeineCacheUtil(Configuration configuration) {
        caffeineCache = Caffeine.newBuilder()
//                .initialCapacity(configuration.getLong(Configs.FILE_META_CACHE_SIZE))
                .maximumWeight(configuration.getLong(Configs.FILE_META_CACHE_SIZE))
                .weigher(new Weigher<String, FileChunk>() {
                    @Override
                    public @NonNegative int weigh(@NonNull String key, @NonNull FileChunk value) {
                        return (int) value.getLength();
                    }
                })
                .expireAfterWrite(configuration.get(Configs.FILE_META_CACHE_TIME), TimeUnit.SECONDS)
                .build();
    }

    public static synchronized CaffeineCacheUtil getInstance(Configuration configuration) {
        if (null == caffeineCacheUtil) {
            caffeineCacheUtil = new CaffeineCacheUtil(configuration);
        }
        return caffeineCacheUtil;
    }

    public Cache<String, FileChunk> getCaffeineCache() {
        return caffeineCache;
    }

    public void close() {
        caffeineCache.cleanUp();
    }
}
