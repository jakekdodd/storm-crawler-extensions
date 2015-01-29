package com.shopstyle.crawler.jsoup;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.http.entity.ContentType;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
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

import com.digitalpebble.storm.crawler.filtering.URLFilters;
import com.digitalpebble.storm.crawler.parse.ParseFilter;
import com.digitalpebble.storm.crawler.parse.ParseFilters;
import com.digitalpebble.storm.crawler.protocol.HttpHeaders;
import com.digitalpebble.storm.crawler.util.ConfUtils;
import com.digitalpebble.storm.crawler.util.KeyValues;
import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;

/**
 * Simple parser for HTML documents which calls ParseFilters to add metadata. Does not handle
 * outlinks for now.
 */
@SuppressWarnings("serial")
public class JSoupParserBolt extends BaseRichBolt {

    private final Logger log = LoggerFactory.getLogger(JSoupParserBolt.class);

    private OutputCollector collector;

    private MultiCountMetric eventCounter;

    private ParseFilter parseFilters = null;

    private URLFilters urlFilters = null;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        this.eventCounter = context.registerMetric("parser_counter", new MultiCountMetric(), 10);

        parseFilters = ParseFilters.emptyParseFilter;

        String parseconfigfile =
                ConfUtils.getString(conf, "parsefilters.config.file", "parsefilters.json");
        if (parseconfigfile != null) {
            try {
                parseFilters = new ParseFilters(conf, parseconfigfile);
            } catch (IOException e) {
                log.error("Exception caught while loading the ParseFilters");
                throw new RuntimeException("Exception caught while loading the ParseFilters", e);
            }
        }

        String urlconfigfile =
                ConfUtils.getString(conf, "urlfilters.config.file", "urlfilters.json");

        if (urlconfigfile != null) {
            try {
                urlFilters = new URLFilters(conf, urlconfigfile);
            } catch (IOException e) {
                log.error("Exception caught while loading the URLFilters");
                throw new RuntimeException("Exception caught while loading the URLFilters", e);
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {

        byte[] content = tuple.getBinaryByField("content");

        String url = tuple.getStringByField("url");
        HashMap<String, String[]> metadata =
                (HashMap<String, String[]>) tuple.getValueByField("metadata");

        if (content == null) {
            log.error("Null content for : " + url);
            // TODO use status stream instead
            collector.fail(tuple);
            eventCounter.scope("failed").incrBy(1);
            return;
        }

        long start = System.currentTimeMillis();

        String text = "";

        Set<String> slinks = Collections.emptySet();

        String charset = null;

        // check if the server specified a charset
        String contentType = KeyValues.getValue(HttpHeaders.CONTENT_TYPE, metadata);
        try {
            ContentType ct = org.apache.http.entity.ContentType.parse(contentType);
            charset = ct.getCharset().name();
        } catch (Exception e) {
            charset = null;
        }

        // filter HTML tags
        CharsetDetector detector = new CharsetDetector();
        detector.enableInputFilter(true);
        // give it a hint
        detector.setDeclaredEncoding(charset);
        detector.setText(content);
        try {
            CharsetMatch charsetMatch = detector.detect();
            if (charsetMatch != null) {
                charset = charsetMatch.getName();
            }
        } catch (Exception e) {
            // ignore and leave the charset as-is
        }

        DocumentFragment fragment;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(content)) {
            org.jsoup.nodes.Document jsoupDoc = Jsoup.parse(bais, charset, url);
            fragment = DOMBuilder.jsoup2HTML(jsoupDoc);

            Elements links = jsoupDoc.select("a[href]");
            slinks = new HashSet<String>(links.size());
            for (Element link : links) {
                String targetURL = link.attr("abs:href");
                // ignore the anchors for now
                // String anchor = link.text();
                if (StringUtils.isNotBlank(targetURL)) {
                    slinks.add(targetURL);
                }
            }

            text = jsoupDoc.body().text();

        } catch (Throwable e) {
            log.error("Exception while parsing {}", url, e);
            collector.fail(tuple);
            eventCounter.scope("failed").incrBy(1);
            return;
        }

        long duration = System.currentTimeMillis() - start;

        log.info("Parsed {} in {} msec", url, duration);

        // apply the parse filters if any
        parseFilters.filter(url, content, fragment, metadata);

        // get the outlinks and convert them to strings (for now)
        URL url_;
        try {
            url_ = new URL(url);
        } catch (MalformedURLException e1) {
            // we would have known by now as previous
            // components check whether the URL is valid
            log.error("MalformedURLException on {}", url);
            collector.fail(tuple);
            return;
        }

        Set<String> linksKept = new HashSet<String>();

        Iterator<String> linkIterator = slinks.iterator();
        while (linkIterator.hasNext()) {
            String targetURL = linkIterator.next();
            // filter the urls
            if (urlFilters != null) {
                targetURL = urlFilters.filter(url_, metadata, targetURL);
                if (targetURL == null) {
                    continue;
                }
            }
            // the link has survived the various filters
            if (targetURL != null) {
                linksKept.add(targetURL);
            }
        }

        eventCounter.scope("parsed").incrBy(1);

        collector.emit(tuple, new Values(url, content, metadata, text.trim(), linksKept));

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // output of this module is the list of fields to index
        // with at least the URL, text content
        declarer.declare(new Fields("url", "content", "metadata", "text", "outlinks"));
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
