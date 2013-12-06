package io.kazuki.v0.internal.v1schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Object class for a counter definition, including name and column definitions.
 */
public class CounterDefinition {
    private final String name;
    private final List<CounterAttribute> counterAttributes;
    private final List<String> attributeNames;

    @JsonCreator
    public CounterDefinition(@JsonProperty("name") String name,
            @JsonProperty("cols") List<CounterAttribute> cols) {
        if (name == null) {
            throw new IllegalArgumentException("'name' must not be null");
        }

        if (cols == null || cols.isEmpty()) {
            throw new IllegalArgumentException("'cols' must not be empty");
        }

        for (CounterAttribute col : cols) {
            if ("id".equals(col.getName()) || "type".equals(col.getName())) {
                throw new IllegalArgumentException(
                        "'cols' must not contain 'id' or 'type' fields");
            }
        }

        this.name = name;

        List<String> newAttributeNames = new ArrayList<String>();
        for (CounterAttribute attr : cols) {
            newAttributeNames.add(attr.getName());
        }

        this.counterAttributes = Collections.unmodifiableList(cols);
        this.attributeNames = Collections.unmodifiableList(newAttributeNames);
    }

    public String getName() {
        return name;
    }

    @JsonProperty("cols")
    public List<CounterAttribute> getCounterAttributes() {
        return counterAttributes;
    }

    @JsonIgnore
    public List<String> getAttributeNames() {
        return attributeNames;
    }
}
