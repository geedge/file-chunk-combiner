package com.zdjizhi.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.zdjizhi.pojo.FileChunk;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.TimeUnit;

public class CaffeineCacheUtil {
    private static CaffeineCacheUtil caffeineCacheUtil = null;
    private static Cache<String, FileChunk> caffeineCache = null;

    private CaffeineCacheUtil(Long cacheSize, Long cacheTimeMs) {
        caffeineCache = Caffeine.newBuilder()
//                .initialCapacity(configuration.getLong(Configs.SINK_OSS_CACHE_SIZE))
                .maximumWeight(cacheSize)
                .weigher(new Weigher<String, FileChunk>() {
                    @Override
                    public @NonNegative int weigh(@NonNull String key, @NonNull FileChunk value) {
                        return (int) value.getLength();
                    }
                })
                .expireAfterWrite(cacheTimeMs, TimeUnit.MILLISECONDS)
                .build();
    }

    public static synchronized CaffeineCacheUtil getInstance(Long cacheSize, Long cacheTimeMs) {
        if (null == caffeineCacheUtil) {
            caffeineCacheUtil = new CaffeineCacheUtil(cacheSize, cacheTimeMs);
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
