package io.kazuki.v0.internal.v1schema;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Attribute class for properties of a user-defined Object.
 */
public class Attribute {
    private final String name;
    private final AttributeType type;
    private final Integer minlength;
    private final Integer maxlength;
    private final List<String> values;
    private final boolean nullable;

    @JsonCreator
    public Attribute(@JsonProperty("name") String name,
            @JsonProperty("type") AttributeType type,
            @JsonProperty("minlength") Integer minlength,
            @JsonProperty("maxlength") Integer maxlength,
            @JsonProperty("values") List<String> values,
            @JsonProperty("nullable") Boolean nullable) {
        if (name == null) {
            throw new IllegalArgumentException("'name' must not be null");
        }

        if (type == null) {
            throw new IllegalArgumentException("'type' must be specified");
        }

        this.name = name;
        this.type = type;
        this.minlength = minlength;
        this.maxlength = maxlength;
        this.nullable = (nullable == null) || nullable;

        if (values != null) {
            values = Collections.unmodifiableList(values);
        }

        this.values = values;
    }

    public String getName() {
        return name;
    }

    public AttributeType getType() {
        return type;
    }

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    public Integer getMaxlength() {
        return maxlength;
    }

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    public Integer getMinlength() {
        return minlength;
    }

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    public List<String> getValues() {
        return values;
    }

    public boolean isNullable() {
        return nullable;
    }
}
