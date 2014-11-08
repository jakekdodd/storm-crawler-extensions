package com.shopstyle.crawler.jsoup;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.digitalpebble.storm.crawler.parse.ParseFilter;
import com.digitalpebble.storm.crawler.parse.ParseFilters;
import com.digitalpebble.storm.crawler.util.ConfUtils;

/**
 * Simple parser for HTML documents which calls ParseFilters to add metadata. Does not handle
 * outlinks for now.
 ***/

@SuppressWarnings("serial")
public class JSoupParserBolt extends BaseRichBolt {

    private final Logger log = LoggerFactory.getLogger(JSoupParserBolt.class);

    private OutputCollector collector;

    private MultiCountMetric eventCounter;

    private ParseFilter parseFilters = null;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;

        this.eventCounter = context.registerMetric("parser_counter", new MultiCountMetric(), 10);

        parseFilters = ParseFilters.emptyParseFilter;

        String parseconfigfile =
                ConfUtils.getString(conf, "parsefilters.config.file", "parsefilters.json");
        if (parseconfigfile != null) {
            try {
                parseFilters = new ParseFilters(parseconfigfile);
            } catch (IOException e) {
                log.error("Exception caught while loading the ParseFilters");
                throw new RuntimeException("Exception caught while loading the ParseFilters", e);
            }
        }
    }

    public void execute(Tuple tuple) {

        byte[] content = tuple.getBinaryByField("content");

        String url = tuple.getStringByField("url");
        HashMap<String, String[]> metadata =
                (HashMap<String, String[]>) tuple.getValueByField("metadata");

        // TODO check status etc...

        if (content == null) {
            log.error("Null content for : " + url);
            collector.fail(tuple);
            eventCounter.scope("failed").incrBy(1);
            return;
        }

        long start = System.currentTimeMillis();

        String text = "";

        DocumentFragment fragment;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(content)) {
            org.jsoup.nodes.Document jsoupDoc = Jsoup.parse(bais, null, url);
            fragment = DOMBuilder.jsoup2HTML(jsoupDoc);

            // StringBuffer sb = new StringBuffer();
            // utils.getText(sb, fragment); // extract text
            // text = sb.toString();
        } catch (Throwable e) {
            log.error("Exception while parsing " + url, e);
            collector.fail(tuple);
            eventCounter.scope("failed").incrBy(1);
            return;
        }

        long duration = System.currentTimeMillis() - start;

        log.info("Parsed " + url + " in " + duration + " msec");

        // apply the parse filters if any
        parseFilters.filter(url, content, fragment, metadata);

        eventCounter.scope("parsed").incrBy(1);

        collector.emit(tuple, new Values(url, content, metadata, text.trim()));

        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // output of this module is the list of fields to index
        // with at least the URL, text content

        declarer.declare(new Fields("url", "content", "metadata", "text"));
    }

    public static void print(Node node, String indent, StringBuffer sb) {
        sb.append(indent).append(node.getClass().getName());
        Node child = node.getFirstChild();
        while (child != null) {
            print(child, indent + " ", sb);
            child = child.getNextSibling();
        }
    }
}
