package com.github.leandroulisses.batch.domain;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;

import java.util.HashMap;
import java.util.Map;

class DynamicFieldSetMapper implements FieldSetMapper {

    public Map<String, String> mapFieldSet(final FieldSet fieldSet) {

        String[] colNames = fieldSet.getNames();
        Map<String, String> map = new HashMap<>();

        for (String colName : colNames) {
            map.put(colName, fieldSet.readString(colName));
        }

        return map;
    }

}
