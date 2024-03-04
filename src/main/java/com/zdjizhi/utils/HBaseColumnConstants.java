package com.zdjizhi.utils;

import org.apache.hadoop.hbase.util.Bytes;

public interface HBaseColumnConstants {
    String FAMILY_DATA = "data";
    String FAMILY_META = "meta";
    String COLUMN_FILENAME = "filename";
    String COLUMN_CONTENT_TYPE = "content_type";
    String COLUMN_CONTENT_LENGTH = "content_length";
    String COLUMN_LAST_MODIFIED = "last_modified";
    String COLUMN_INDEX_FILENAME_KEY = "index_filename_key";
    String COLUMN_INDEX_TIME_KEY = "index_time_key";
    String COLUMN_UPLOAD_METHOD = "upload_method";
    String COLUMN_STORAGE_BACKEND = "storage_backend";
    String COLUMN_FILE_PATH = "file_path";
    String COLUMN_APPEND_MODE = "append_mode";
    String COLUMN_APPEND_PART_SIZE = "append_part_size";
    String COLUMN_APPEND_PART_CHUNK_COUNT = "append_part_chunk_count";
    String COLUMN_COMBINE_MODE = "combine_mode";
    String COLUMN_USER_DEFINED_META_PREFIX = "user_defined_meta_";
    String BUCKET_COLUMN_USER = "user";
    String BUCKET_COLUMN_PRIVILEGE = "privilege";
    String BUCKET_COLUMN_TTL = "ttl";
    String BUCKET_COLUMN_WAL = "wal";
    String BUCKET_COLUMN_LOCATION = "location";

    byte[] BYTE_FAMILY_DATA = Bytes.toBytes(FAMILY_DATA);
    byte[] BYTE_FAMILY_META = Bytes.toBytes(FAMILY_META);
    byte[] BYTE_COLUMN_FILENAME = Bytes.toBytes(COLUMN_FILENAME);
    byte[] BYTE_COLUMN_CONTENT_TYPE = Bytes.toBytes(COLUMN_CONTENT_TYPE);
    byte[] BYTE_COLUMN_CONTENT_LENGTH = Bytes.toBytes(COLUMN_CONTENT_LENGTH);
    byte[] BYTE_COLUMN_LAST_MODIFIED = Bytes.toBytes(COLUMN_LAST_MODIFIED);
    byte[] BYTE_COLUMN_INDEX_FILENAME_KEY = Bytes.toBytes(COLUMN_INDEX_FILENAME_KEY);
    byte[] BYTE_COLUMN_INDEX_TIME_KEY = Bytes.toBytes(COLUMN_INDEX_TIME_KEY);
    byte[] BYTE_COLUMN_UPLOAD_METHOD = Bytes.toBytes(COLUMN_UPLOAD_METHOD);
    byte[] BYTE_COLUMN_STORAGE_BACKEND = Bytes.toBytes(COLUMN_STORAGE_BACKEND);
    byte[] BYTE_COLUMN_FILE_PATH = Bytes.toBytes(COLUMN_FILE_PATH);
    byte[] BYTE_COLUMN_APPEND_MODE = Bytes.toBytes(COLUMN_APPEND_MODE);
    byte[] BYTE_COLUMN_APPEND_PART_SIZE = Bytes.toBytes(COLUMN_APPEND_PART_SIZE);
    byte[] BYTE_COLUMN_APPEND_PART_CHUNK_COUNT = Bytes.toBytes(COLUMN_APPEND_PART_CHUNK_COUNT);
    byte[] BYTE_COLUMN_COMBINE_MODE = Bytes.toBytes(COLUMN_COMBINE_MODE);
    byte[] BYTE_BUCKET_COLUMN_USER = Bytes.toBytes(BUCKET_COLUMN_USER);
    byte[] BYTE_BUCKET_COLUMN_PRIVILEGE = Bytes.toBytes(BUCKET_COLUMN_PRIVILEGE);
    byte[] BYTE_BUCKET_COLUMN_TTL = Bytes.toBytes(BUCKET_COLUMN_TTL);
    byte[] BYTE_BUCKET_COLUMN_WAL = Bytes.toBytes(BUCKET_COLUMN_WAL);
    byte[] BYTE_BUCKET_COLUMN_LOCATION = Bytes.toBytes(BUCKET_COLUMN_LOCATION);
}
