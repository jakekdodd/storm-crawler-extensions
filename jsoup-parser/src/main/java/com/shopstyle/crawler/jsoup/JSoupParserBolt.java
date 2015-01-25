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
import com.digitalpebble.storm.crawler.util.ConfUtils;

import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;

import crawlercommons.url.PaidLevelDomain;

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

    private boolean ignoreOutsideHost = false;
    private boolean ignoreOutsideDomain = false;

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
                urlFilters = new URLFilters(urlconfigfile);
            } catch (IOException e) {
                log.error("Exception caught while loading the URLFilters");
                throw new RuntimeException("Exception caught while loading the URLFilters", e);
            }
        }

        ignoreOutsideHost =
                ConfUtils.getBoolean(conf, "parser.ignore.outlinks.outside.host", false);
        ignoreOutsideDomain =
                ConfUtils.getBoolean(conf, "parser.ignore.outlinks.outside.domain", false);

    }

    @Override
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

        Set<String> slinks = Collections.emptySet();

        DocumentFragment fragment;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(content)) {
            org.jsoup.nodes.Document jsoupDoc = Jsoup.parse(bais, null, url);
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
        String fromHost;
        String fromDomain;
        URL url_;
        try {
            url_ = new URL(url);
            fromHost = url_.getHost().toLowerCase();
            fromDomain = PaidLevelDomain.getPLD(fromHost);
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
            // resolve the host of the target
            String toHost = null;
            try {
                URL tmpURL = new URL(targetURL);
                toHost = tmpURL.getHost();
            } catch (MalformedURLException e) {
                log.debug("MalformedURLException on {}", targetURL);
                continue;
            }

            // filter the urls
            if (urlFilters != null) {
                targetURL = urlFilters.filter(targetURL);
                if (targetURL == null) {
                    continue;
                }
            }

            if (targetURL != null && ignoreOutsideHost) {
                if (toHost == null || !toHost.equals(fromHost)) {
                    targetURL = null; // skip it
                    continue;
                }
            }

            if (targetURL != null && ignoreOutsideDomain) {
                String toDomain;
                try {
                    toDomain = PaidLevelDomain.getPLD(toHost);
                } catch (Exception e) {
                    toDomain = null;
                }
                if (toDomain == null || !toDomain.equals(fromDomain)) {
                    targetURL = null; // skip it
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
