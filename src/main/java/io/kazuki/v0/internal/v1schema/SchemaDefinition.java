//package io.kazuki.v0.internal.v1schema;
//
//import java.util.Collections;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//
//import com.fasterxml.jackson.annotation.JsonCreator;
//import com.fasterxml.jackson.annotation.JsonIgnore;
//import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
//import com.fasterxml.jackson.annotation.JsonProperty;
//
///**
// * Schema definition class - the main entry point for schema creation.
// */
//@JsonIgnoreProperties(ignoreUnknown = true)
//public class SchemaDefinition<T> {
//  private final List<Attribute> attributes;
//  private final Map<String, Attribute> attributeMap;
//
//  @JsonCreator
//  public SchemaDefinition(@JsonProperty("attributes") List<Attribute> attributes) {
//    if (attributes == null) {
//      throw new IllegalArgumentException("'attributes' must be present");
//    }
//
//    this.attributes = Collections.unmodifiableList(attributes);
//
//    Map<String, Attribute> newAttributes = new LinkedHashMap<String, Attribute>();
//    for (Attribute attr : attributes) {
//      newAttributes.put(attr.getName(), attr);
//    }
//
//    this.attributeMap = Collections.unmodifiableMap(newAttributes);
//  }
//
//  public List<Attribute> getAttributes() {
//    return attributes;
//  }
//
//  @JsonIgnore
//  public Map<String, Attribute> getAttributesMap() {
//    return attributeMap;
//  }
//}
