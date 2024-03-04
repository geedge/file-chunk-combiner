package com.zdjizhi.utils;

import com.zdjizhi.config.Configs;
import org.apache.flink.configuration.Configuration;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;

import javax.net.ssl.*;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

public class HttpClientUtil {

    private static HttpClientUtil httpClientUtil;
    private final Configuration configuration;

    private HttpClientUtil(Configuration configuration) {
        this.configuration = configuration;
    }

    private RequestConfig getRequestConfig() {
        return RequestConfig.custom()
                .setConnectTimeout(configuration.get(Configs.SINK_HOS_HTTP_CONNECT_TIMEOUT))
                .setConnectionRequestTimeout(configuration.get(Configs.SINK_HOS_HTTP_REQUEST_TIMEOUT))
                .setSocketTimeout(configuration.get(Configs.SINK_HOS_HTTP_SOCKET_TIMEOUT))
                .build();
    }

    private HttpRequestRetryHandler getRetryHandler() {
        return (exception, executionCount, context) -> {
            if (executionCount >= configuration.get(Configs.SINK_HOS_HTTP_ERROR_RETRY)) {
                return false;
            }
            if (exception instanceof NoHttpResponseException) {// 如果服务器丢掉了连接，那么就重试
                return true;
            }
            if (exception instanceof SSLHandshakeException) {// 不要重试SSL握手异常
                return false;
            }
            if (exception instanceof ConnectException) {// 连接被拒绝
                return false;
            }
            if (exception instanceof InterruptedIOException) {// 超时
                return true;
            }
            if (exception instanceof UnknownHostException) {// 目标服务器不可达
                return false;
            }
            if (exception instanceof SSLException) {// ssl握手异常
                return false;
            }
            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            // 如果请求是幂等的，就再次尝试
            return !(request instanceof HttpEntityEnclosingRequest);
        };
    }

    private PoolingHttpClientConnectionManager getSyncSslClientManager() {
        PoolingHttpClientConnectionManager connManager;
        try {
            X509TrustManager trustManager = new X509TrustManager() {
                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                @Override
                public void checkClientTrusted(X509Certificate[] xcs, String str) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] xcs, String str) {
                }
            };
            SSLContext ctx = SSLContext.getInstance(SSLConnectionSocketFactory.TLS);
            ctx.init(null, new TrustManager[]{trustManager}, null);
            SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(ctx, NoopHostnameVerifier.INSTANCE);
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.INSTANCE)
                    .register("https", socketFactory).build();
            // 创建ConnectionManager，添加Connection配置信息
            connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            // 设置最大连接数
            connManager.setMaxTotal(configuration.get(Configs.SINK_HOS_HTTP_MAX_TOTAL));
            // 设置每个连接的路由数
            connManager.setDefaultMaxPerRoute(configuration.get(Configs.SINK_HOS_HTTP_MAX_PER_ROUTE));
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
        return connManager;
    }

    private PoolingNHttpClientConnectionManager getAsyncSslClientManager() {
        PoolingNHttpClientConnectionManager connManager;
        try {
//            X509TrustManager trustManager = new X509TrustManager() {
//                @Override
//                public X509Certificate[] getAcceptedIssuers() {
//                    return null;
//                }
//
//                @Override
//                public void checkClientTrusted(X509Certificate[] xcs, String str) {
//                }
//
//                @Override
//                public void checkServerTrusted(X509Certificate[] xcs, String str) {
//                }
//            };
//            SSLContext sslContext = SSLContext.getInstance(SSLConnectionSocketFactory.TLS);
//            sslContext.init(null, new TrustManager[]{trustManager}, null);
//            SSLIOSessionStrategy sslioSessionStrategy = new SSLIOSessionStrategy(sslContext, SSLIOSessionStrategy.ALLOW_ALL_HOSTNAME_VERIFIER);
//            SSLIOSessionStrategy defaultStrategy = SSLIOSessionStrategy.getDefaultStrategy();
//            Registry<SchemeIOSessionStrategy> schemeIOSessionStrategyRegistry = RegistryBuilder.<SchemeIOSessionStrategy>create()
//                    .register("http", NoopIOSessionStrategy.INSTANCE)
//                    .register("https", sslioSessionStrategy).build();

            IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                    .setIoThreadCount(Runtime.getRuntime().availableProcessors())
                    .setSoKeepAlive(true)
                    .build();
            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
            connManager = new PoolingNHttpClientConnectionManager(ioReactor);
            connManager.setMaxTotal(configuration.get(Configs.SINK_HOS_HTTP_MAX_TOTAL));
            connManager.setDefaultMaxPerRoute(configuration.get(Configs.SINK_HOS_HTTP_MAX_PER_ROUTE));
        } catch (IOReactorException e) {
            throw new RuntimeException(e.getMessage());
        }
        return connManager;
    }

    public static synchronized HttpClientUtil getInstance(Configuration configuration) {
        if (null == httpClientUtil) {
            httpClientUtil = new HttpClientUtil(configuration);
        }
        return httpClientUtil;
    }

    public CloseableHttpClient getSyncHttpClient() {
        return HttpClients.custom()
                .setDefaultRequestConfig(getRequestConfig())
                .setRetryHandler(getRetryHandler())
                .setConnectionManager(getSyncSslClientManager())
                .build();
    }

    public CloseableHttpAsyncClient getAsyncHttpClient() {
        return HttpAsyncClients.custom()
                .setDefaultRequestConfig(getRequestConfig())
                .setConnectionManager(getAsyncSslClientManager())
                .build();
    }
}
