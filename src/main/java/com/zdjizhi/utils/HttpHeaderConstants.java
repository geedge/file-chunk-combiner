package com.zdjizhi.utils;

public interface HttpHeaderConstants {
    /*
     * Standard HTTP Headers
     */
    String CACHE_CONTROL = "Cache-Control";
    String CONTENT_DISPOSITION = "Content-Disposition";
    String CONTENT_ENCODING = "Content-Encoding";
    String CONTENT_LENGTH = "Content-Length";
    String CONTENT_RANGE = "Content-Range";
    String CONTENT_MD5 = "Content-MD5";
    String CONTENT_TYPE = "Content-Type";
    String CONTENT_LANGUAGE = "Content-Language";
    String DATE = "Date";
    String LAST_MODIFIED = "Last-Modified";
    String SERVER = "Server";
    String CONNECTION = "Connection";
    String ETAG = "ETag";

    /*
     * Hos HTTP Headers
     */
    String AMZ_ACL = "x-amz-acl";
    String AMZ_CONTENT_SHA_256 = "x-amz-content-sha256";
    String AMZ_DATE = "x-amz-date";
    String HOS_WAL = "x-hos-wal";
    String HOS_COMPRESSION = "x-hos-compression";
    String HOS_SPLITS = "x-hos-splits";
    String HOS_UPLOAD_TYPE = "x-hos-upload-type";
    String HOS_START_TIME = "x-hos-start-time";
    String HOS_END_TIME = "x-hos-end-time";
    String HOS_QUICK = "x-hos-quick";
    String HOS_OBJECT_INFO = "x-hos-object-info";
    String TOKEN = "token";
    String HOS_POSITION = "x-hos-position";
    String HOS_NEXT_APPEND_POSITION = "x-hos-next-append-position";
    String HOS_COMBINE_MODE = "x-hos-combine-mode";
    String HOS_PART_NUMBER = "x-hos-part-number";
    String HOS_OFFSET = "x-hos-offset";
    String HOS_PART_LAST_FLAG = "x-hos-part-last-flag";
    String HOS_PART_CHUNK_COUNT = "x-hos-part-chunk-count";
    String HOS_PART_CHUNK_NUMBERS = "x-hos-part-chunk-numbers";
    String HOS_META_FILE_TYPE = "x-hos-meta-file-type";
    String HOS_META_FILENAME = "x-hos-meta-filename";
    String FILE_SIZE = "File-Size";
    String LOCATION = "Location";
    String HOS_OBJECT_TYPE = "x-hos-object-type";
    String HOS_APPEND_INFO = "x-hos-append-info";
    String HOS_META_PREFIX = "x-hos-meta-";
    String HOS_METADATA_DIRECTIVE = "x-hos-metadata-directive";
    String HOS_OBJECTS_META = "x-hos-objects-meta";
    String HOS_OBJECTS_OFFSET = "x-hos-objects-offset";
}
