package com.dtstack.flink.sql.sink.dorisdb.manager;

import com.alibaba.fastjson.JSON;
import com.dtstack.flink.sql.sink.dorisdb.row.DorisdbDelimiterParser;
import com.dtstack.flink.sql.sink.dorisdb.row.DorisdbSinkOP;
import com.dtstack.flink.sql.sink.dorisdb.table.DorisdbSinkOptions;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import java.io.IOException;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DorisdbStreamLoadVisitor implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DorisdbStreamLoadVisitor.class);

    private final DorisdbSinkOptions sinkOptions;
    private final String[] fieldNames;
    private int pos;

    public DorisdbStreamLoadVisitor(DorisdbSinkOptions sinkOptions, String[] fieldNames) {
        this.fieldNames = fieldNames;
        this.sinkOptions = sinkOptions;
    }

    public void doStreamLoad(Tuple3<String, Long, ArrayList<byte[]>> labeledRows) throws IOException {
        String host = getAvailableHost();
        if (null == host) {
            throw new IOException("None of the hosts in `load_url` could be connected.");
        }
        String loadUrl = new StringBuilder(host)
                .append("/api/")
                .append(sinkOptions.getDatabaseName())
                .append("/")
                .append(sinkOptions.getTableName())
                .append("/_stream_load")
                .toString();
        LOG.info(String.format("Start to join batch data: rows[%d] bytes[%d] label[%s].", labeledRows._3().size(), labeledRows._2(), labeledRows._1()));
        Map<String, Object> loadResult = doHttpPut(loadUrl, labeledRows._1(), joinRows(labeledRows._3(), labeledRows._2().intValue()));
        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw new IOException("Unable to flush data to Dorisdb: unknown result status, usually caused by authentication or permission related problems.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
        }
        if (loadResult.get(keyStatus).equals("Fail")) {
            LOG.error(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
            throw new DorisdbStreamLoadFailedException(String.format("Failed to flush data to Dorisdb, Error response: \n%s\n", JSON.toJSONString(loadResult)), loadResult);
        }
    }

    private String getAvailableHost() {
        List<String> hostList = sinkOptions.getLoadUrlList();
        if (pos >= hostList.size()) {
            pos = 0;
        }
        for (; pos < hostList.size(); pos++) {
            String host = new StringBuilder("http://").append(hostList.get(pos)).toString();
            if (tryHttpConnection(host)) {
                return host;
            }
        }
        return null;
    }

    private boolean tryHttpConnection(String host) {
        try {
            URL url = new URL(host);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(sinkOptions.getConnectTimout());
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            LOG.warn("Failed to connect to address:{}", host, e1);
            return false;
        }
    }

    private byte[] joinRows(List<byte[]> rows, int totalBytes) throws IOException {
        if (DorisdbSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat())) {
            byte[] lineDelimiter = DorisdbDelimiterParser.parse(sinkOptions.getSinkStreamLoadProperties().get("row_delimiter"), "\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
            for (byte[] row : rows) {
                bos.put(row);
                bos.put(lineDelimiter);
            }
            return bos.array();
        }

        if (DorisdbSinkOptions.StreamLoadFormat.JSON.equals(sinkOptions.getStreamLoadFormat())) {
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + (rows.isEmpty() ? 2 : rows.size() + 1));
            bos.put("[".getBytes(StandardCharsets.UTF_8));
            byte[] jsonDelimiter = ",".getBytes(StandardCharsets.UTF_8);
            boolean isFirstElement = true;
            for (byte[] row : rows) {
                if (!isFirstElement) {
                    bos.put(jsonDelimiter);
                }
                bos.put(row);
                isFirstElement = false;
            }
            bos.put("]".getBytes(StandardCharsets.UTF_8));
            return bos.array();
        }
        throw new RuntimeException("Failed to join rows data, unsupported `format` from stream load properties:");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> doHttpPut(String loadUrl, String label, byte[] data) throws IOException {
        LOG.info(String.format("Executing stream load to: '%s', size: '%s'", loadUrl, data.length));
        final HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });
        try (CloseableHttpClient httpclient = httpClientBuilder.build()) {
            HttpPut httpPut = new HttpPut(loadUrl);
            Map<String, String> props = sinkOptions.getSinkStreamLoadProperties();
            for (Map.Entry<String, String> entry : props.entrySet()) {
                httpPut.setHeader(entry.getKey(), entry.getValue());
            }
            if (!props.containsKey("columns") && (sinkOptions.supportUpsertDelete() || DorisdbSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat()))) {
                String cols = String.join(",", Arrays.asList(fieldNames).stream().map(f -> String.format("`%s`", f.trim().replace("`", ""))).collect(Collectors.toList()));
                if (cols.length() > 0 && sinkOptions.supportUpsertDelete()) {
                    cols += String.format(",%s", DorisdbSinkOP.COLUMN_KEY);
                }
                httpPut.setHeader("columns", cols);
            }
            httpPut.setHeader("Expect", "100-continue");
            httpPut.setHeader("label", label);
            httpPut.setHeader("Content-Type", "application/x-www-form-urlencoded");
            httpPut.setHeader("Authorization", getBasicAuthHeader(sinkOptions.getUsername(), sinkOptions.getPassword()));
            httpPut.setEntity(new ByteArrayEntity(data));
            httpPut.setConfig(RequestConfig.custom().setRedirectsEnabled(true).build());
            try (CloseableHttpResponse resp = httpclient.execute(httpPut)) {
                int code = resp.getStatusLine().getStatusCode();
                if (200 != code) {
                    LOG.warn("Request failed with code:{}", code);
                    return null;
                }
                HttpEntity respEntity = resp.getEntity();
                if (null == respEntity) {
                    LOG.warn("Request failed with empty response.");
                    return null;
                }
                return (Map<String, Object>) JSON.parse(EntityUtils.toString(respEntity));
            }
        }
    }

    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
    }

}
