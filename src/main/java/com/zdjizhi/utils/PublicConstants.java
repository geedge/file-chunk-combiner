package com.zdjizhi.utils;

public interface PublicConstants {
    String STORAGE_BACKEND_HBASE = "hbase";
    String STORAGE_BACKEND_FILESYSTEM = "filesystem";
    String APPEND_MODE_OFFSET = "offset";
    String APPEND_MODE_PART_NUMBER = "partNumber";
    String COMBINE_MODE_APPEND = "append";
    String COMBINE_MODE_SEEK = "seek";
    String MULTIPART_FILE_PART_MESSAGE_ETAG = "etag";
    String MULTIPART_FILE_PART_MESSAGE_SIZE = "partSize";
    String APPEND_FILE_PART_MESSAGE_SIZE = "partSize";
    String APPEND_FILE_PART_MESSAGE_CHUNK_COUNT = "chunkCount";
    String APPEND_FILE_PART_MESSAGE_LAST_PART_FLAG = "lastPartFlag";
    String APPEND_FILE_PART_MESSAGE_CHUNK_NUMBERS = "chunkNumbers";
    String BUCKET_META_PRIVILEGE_PRIVATE = "private";
    String BUCKET_META_PRIVILEGE_PUBLIC = "public-read-write";
    String DEFAULT_BUCKET_META_WAL = "close";
    String UPLOAD_TYPE_UPDATE = "put";
    String UPLOAD_TYPE_APPENDV2 = "appendV2";
    String UPLOAD_TYPE_APPEND = "append";
    String DEFAULT_GET_FILE_LIST_QUICK = "false";
    String FILE_DATA_ROW_SUFFIX = "0";
    String FILE_DATA_COLUMN = "0";
    String FILE_TYPE_PACPNG = "pcapng";
    String FILE_TYPE_PACP = "pcap";
    String FILE_META_FILE_TYPE = "file_type";
    String FILE_META_FILENAME = "filename";
    String OBJECT_TYPE_NORMAL = "Normal";
    String OBJECT_TYPE_APPENDABLE = "Appendable";
    String OBJECT_TYPE_MULTIPART = "Multipart";
    String DEFAULT_METADATA_DIRECTIVE = "REPLACE_NEW";
}
