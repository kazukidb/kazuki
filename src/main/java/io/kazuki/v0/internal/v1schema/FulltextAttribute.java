package io.kazuki.v0.internal.v1schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class for full-text index column specifications.
 */
public class FulltextAttribute {
    public final String name;
    public final AttributeTransform transform;

    @JsonCreator
    public FulltextAttribute(@JsonProperty("name") String name,
            @JsonProperty("transform") AttributeTransform transform) {
        if (name == null) {
            throw new IllegalArgumentException("'name' must not be null");
        }

        this.name = name;
        this.transform = (transform == null) ? AttributeTransform.NONE
                : transform;
    }

    public String getName() {
        return name;
    }

    public AttributeTransform getTransform() {
        return transform;
    }
}
