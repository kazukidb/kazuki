//package io.kazuki.v0.internal.v1schema;
//
//import java.util.LinkedHashMap;
//import java.util.Map;
//
//import javax.xml.bind.ValidationException;
//
//public class SchemaDefinitionValidator {
//
//  public void validate(SchemaDefinition<?> schemaDefinition) throws ValidationException {
//    Map<String, Attribute> theAtts = new LinkedHashMap<String, Attribute>();
//
//    for (Attribute attribute : schemaDefinition.getAttributes()) {
//      SchemaValidationHelper.validateAttribute(attribute);
//      theAtts.put(attribute.getName(), attribute);
//    }
//  }
//
//  public void validateUpgrade(SchemaDefinition<?> oldSchema, SchemaDefinition<?> newSchema) {
//    Map<String, Attribute> newAttrs = newSchema.getAttributesMap();
//    Map<String, Attribute> oldAttrs = oldSchema.getAttributesMap();
//
//    // validate existing attributes
//    for (Attribute oldAttr : oldAttrs.values()) {
//      Attribute newAttr = newAttrs.get(oldAttr.getName());
//      SchemaValidationHelper.validateAttributeUpgrade(oldAttr, newAttr);
//    }
//
//    // tricky note: 'new' attributes may already exist in the entities;
//    // if those entities are incompatible, they will become irretrievable,
//    // and related index, counter, or fulltext rebuilds may fail
//  }
//}
