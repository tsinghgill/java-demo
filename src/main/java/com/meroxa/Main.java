package com.meroxa;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.meroxa.turbine.Turbine;
import com.meroxa.turbine.TurbineApp;
import com.meroxa.turbine.TurbineRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class Main implements TurbineApp {

    @Override
    public void setup(Turbine turbine) {
        turbine
            .resource("source_name")
            .read("collection_name", null)
            .process(this::process)
            .writeTo(turbine.resource("destination_name"), "collection_enriched", null);
    }

    private List<TurbineRecord> process(List<TurbineRecord> records) {
        List<TurbineRecord> processedStream = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();

        for (TurbineRecord record : records) {
            String payload = record.getPayload();

            try {
                Map<String, Object> payloadMap = mapper.readValue(payload, new TypeReference<Map<String, Object>>() {});
                Map<String, String> sourceMap = (Map<String, String>)payloadMap.get("source");
                if (sourceMap != null) {
                    String source = sourceMap.get("name");
                    if ("west-store-mongo".equals(source)) {
                        payloadMap.put("storeId", "001");
                        String updatedPayload = mapper.writeValueAsString(payloadMap);
                        record.setPayload(updatedPayload);
                        processedStream.add(record);
                    }
                }
            } catch (IOException e) {
                System.out.println("Error while processing record: " + e.getMessage());
            }
        }

        return processedStream;
    }
}