package com.shopstyle.crawler.microdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.any23.extractor.microdata.ItemProp;
import org.apache.any23.extractor.microdata.ItemPropValue;
import org.apache.any23.extractor.microdata.ItemScope;
import org.apache.any23.extractor.microdata.MicrodataParser;
import org.apache.any23.extractor.microdata.MicrodataParserException;
import org.apache.any23.extractor.microdata.MicrodataParserReport;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;

import com.digitalpebble.storm.crawler.Metadata;
import com.digitalpebble.storm.crawler.parse.ParseFilter;
import com.digitalpebble.storm.crawler.parse.Outlink;
import com.fasterxml.jackson.databind.JsonNode;

public class MicrodataFilter implements ParseFilter {
    private Logger log = LoggerFactory.getLogger(getClass());
    private boolean includeErrors;
    private boolean useUniquePrefixForNestedValues;

    @Override
    public void configure(Map stormConf, JsonNode paramNode) {
        JsonNode includeErrorField = paramNode.get("includeErrors");
        includeErrors = includeErrorField != null && includeErrorField.asBoolean();
        JsonNode uniquePrefixField = paramNode.get("useUniquePrefixForNestedValues");
        useUniquePrefixForNestedValues = uniquePrefixField != null && uniquePrefixField.asBoolean();
    }

    @Override
    public boolean needsDOM() {
        return true;
    }

    @Override
    public void filter(String URL, byte[] content, DocumentFragment doc, Metadata metadata,
            List<Outlink> outLinks) {
        MicrodataParserReport report;
        try {
            report = getMicrodata(doc);
        } catch (MicrodataParserException e) {
            log.error("Error parsing microdata {}", URL, e);
            return;
        }
        for (ItemScope itemScope : report.getDetectedItemScopes()) {
            addItemScopeToMetadata(itemScope, "microdata." + getShortItemType(itemScope) + '.',
                    metadata);
        }
        if (includeErrors) {
            MicrodataParserException[] errors = report.getErrors();
            List<String> errorMessages = new ArrayList<>(errors.length);
            for (MicrodataParserException error : errors) {
                errorMessages.add(error.getMessage());
            }
            metadata.addValues("microdata.errors", errorMessages);
        }
    }

    private MicrodataParserReport getMicrodata(DocumentFragment document)
            throws MicrodataParserException {
        // Same as MicrodataParser.getTopLevelItemScopeNodes(Node)
        // But doesn't remove the nested ones
        final List<Node> itemScopeNodes = MicrodataParser.getItemScopeNodes(document);
        final List<Node> topLevelItemScopes = new ArrayList<Node>();
        for (Node itemScope : itemScopeNodes) {
            if (!MicrodataParser.isItemProp(itemScope)) {
                topLevelItemScopes.add(itemScope);
            }
        }

        final List<ItemScope> items = new ArrayList<ItemScope>();
        final MicrodataParser microdataParser = new MicrodataParser(document.getOwnerDocument());
        for (Node itemNode : topLevelItemScopes) {
            items.add(microdataParser.getItemScope(itemNode));
        }
        return new MicrodataParserReport(items.toArray(new ItemScope[items.size()]),
                microdataParser.getErrors());
    }

    private void addItemScopeToMetadata(ItemScope itemScope, String outerPrefix, Metadata metadata) {
        Map<String, MutableInt> nestedPrefixIdByType = null;
        Map<String, List<ItemProp>> properties = itemScope.getProperties();
        for (String propertyName : properties.keySet()) {
            List<ItemProp> itemProps = properties.get(propertyName);
            List<String> itemValues = new ArrayList<>(itemProps.size());
            for (ItemProp itemProp : itemProps) {
                ItemPropValue itemPropValue = itemProp.getValue();
                if (itemPropValue == null) {
                    continue;
                } else if (itemPropValue.isNested()) {
                    StringBuilder nestedPrefix =
                            new StringBuilder(outerPrefix).append(propertyName).append('.');
                    if (useUniquePrefixForNestedValues) {
                        if (nestedPrefixIdByType == null) {
                            nestedPrefixIdByType = new HashMap<>();
                        }
                        MutableInt prefixId = nestedPrefixIdByType.get(propertyName);
                        if (prefixId == null) {
                            prefixId = new MutableInt();
                            nestedPrefixIdByType.put(propertyName, prefixId);
                        } else {
                            prefixId.increment();
                        }
                        nestedPrefix.append(prefixId).append('.');
                    }
                    addItemScopeToMetadata(itemPropValue.getAsNested(), nestedPrefix.toString(),
                            metadata);
                } else if (itemPropValue.isDate()) {
                    // Simplest way...
                    itemValues.add(String.valueOf(itemPropValue.getAsDate().getTime()));
                } else {
                    itemValues.add((String) itemPropValue.getContent());
                }
            }
            metadata.addValues(outerPrefix + propertyName, itemValues);
        }
    }

    private String getShortItemType(ItemScope itemScope) {
        String path = itemScope.getType().getPath();
        if (StringUtils.isEmpty(path)) {
            return "unknown";
        } else {
            int lastSlash = path.lastIndexOf('/');
            return path.substring(lastSlash + 1).toLowerCase();
        }
    }
}
