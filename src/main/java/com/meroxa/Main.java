package com.meroxa;

import com.meroxa.turbine.Turbine;
import com.meroxa.turbine.TurbineApp;
import com.meroxa.turbine.TurbineRecord;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

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
        return records.stream()
            .filter(turbineRecord -> {
                String source = (String) turbineRecord.jsonGet("$.payload.after.source");
                return "west-store-mongo".equals(source);
            })
            .map(record -> {
                record.jsonSet("$.payload.after.source", "store-id-001");
                return record;
            })
            .toList();
    }
}
