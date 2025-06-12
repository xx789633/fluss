package com.alibaba.fluss.lake.lance;

import com.lancedb.lance.WriteParams;
import java.util.HashMap;
import java.util.Map;

/** Lance Configuration. */
public class LanceConfig {
    private static final long serialVersionUID = 827364827364823764L;

    private static final String max_row_per_file = "max_row_per_file";
    private static final String max_rows_per_group = "max_rows_per_group";
    private static final String max_bytes_per_file = "max_bytes_per_file";
    private static final String ak = "access_key_id";
    private static final String sk = "secret_access_key";
    private static final String endpoint = "aws_endpoint";
    private static final String region = "aws_region";
    private static final String virtual_hosted_style = "virtual_hosted_style_request";
    private static final String allow_http = "allow_http";

    private static final Map<String, String> options;

    public LanceConfig from(Map<String, String> properties, String datasetUri) {

    }

    public WriteParams genWriteParamsFromConfig() {
        WriteParams.Builder builder = new WriteParams.Builder();
        if (options.containsKey(max_row_per_file)) {
            builder.withMaxRowsPerFile(Integer.parseInt(options.get(max_row_per_file)));
        }
        if (options.containsKey(max_rows_per_group)) {
            builder.withMaxRowsPerGroup(Integer.parseInt(options.get(max_rows_per_group)));
        }
        if (options.containsKey(max_bytes_per_file)) {
            builder.withMaxBytesPerFile(Long.parseLong(options.get(max_bytes_per_file)));
        }
        builder.withStorageOptions(genStorageOptions());
        return builder.build();
    }

    private Map<String, String> genStorageOptions() {
        Map<String, String> storageOptions = new HashMap<>();
        if (options.containsKey(ak) && options.containsKey(sk) && options.containsKey(endpoint)) {
            storageOptions.put(ak, options.get(ak));
            storageOptions.put(sk, options.get(sk));
            storageOptions.put(endpoint, options.get(endpoint));
        }
        if (options.containsKey(region)) {
            storageOptions.put(region, options.get(region));
        }
        if (options.containsKey(virtual_hosted_style)) {
            storageOptions.put(virtual_hosted_style, options.get(virtual_hosted_style));
        }
        if (options.containsKey(allow_http)) {
            storageOptions.put(allow_http, options.get(allow_http));
        }
        return storageOptions;
    }
}
