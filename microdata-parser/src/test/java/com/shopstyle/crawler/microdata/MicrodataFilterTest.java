package com.shopstyle.crawler.microdata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.digitalpebble.storm.crawler.util.KeyValues;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.apache.html.dom.HTMLDocumentImpl;
import org.cyberneko.html.parsers.DOMFragmentParser;
import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.DocumentFragment;
import org.xml.sax.InputSource;

public class MicrodataFilterTest
{
    @Test
    public void testDillardParsing() throws Exception
    {
    	String url = "http://www.dillards.com/product/MICHAEL-Michael-Kors-Kempton-Small-Tote_301_-1_301_503911007";
    	String configFile = "MicrodataFilterTest-mergeNestedValues.json";
    	String contentFile = "dillards.com_503911007.html";
    	Map<String, String[]> metadata = parse(url, configFile, contentFile);
        System.out.println(KeyValues.toString(metadata, null));
        Assert.assertEquals("MICHAEL Michael Kors Kempton Small Tote", KeyValues.getValue("microdata.product.name", metadata));
        Assert.assertEquals("04044499", KeyValues.getValue("microdata.product.identifier", metadata));
        Assert.assertEquals("MICHAEL Michael Kors ►", KeyValues.getValue("microdata.product.brand", metadata));
    }

    @Test
    public void testMacysParsingWithMergeNestedValues() throws Exception
    {
    	String url = "http://www1.macys.com/shop/product/calvin-klein-animal-print-faux-leather-inset-dress?ID=1668293&CategoryID=5449";
    	String configFile = "MicrodataFilterTest-mergeNestedValues.json";
    	String contentFile = "macy.com_1668293.html";
    	Map<String, String[]> metadata = parse(url, configFile, contentFile);
        System.out.println(KeyValues.toString(metadata, null));
        System.out.println(KeyValues.toString(metadata, null));
        Assert.assertEquals("Women - Dresses",
            KeyValues.getValue("microdata.webpage.breadcrumb", metadata));
        Assert.assertEquals("Calvin Klein Animal-Print Faux-Leather-Inset Dress",
            KeyValues.getValue("microdata.product.name", metadata));
        Assert.assertEquals("1668293", KeyValues.getValue("microdata.product.productID", metadata));
        Assert.assertEquals(
            "http://slimages.macys.com/is/image/MCY/products/3/optimized/2338853_fpx.tif"
                    + "?wid=59&hei=72&fit=fit,1&$filtersm$",
            KeyValues.getValue("microdata.product.image", metadata));
        Assert.assertEquals("$74.99",
            KeyValues.getValue("microdata.product.offers.price", metadata));
        Assert.assertEquals("USD",
            KeyValues.getValue("microdata.product.offers.priceCurrency", metadata));
        Assert.assertEquals("http://schema.org/InStock",
            KeyValues.getValue("microdata.product.offers.availability", metadata));
    }

    @Test
    public void testMacysParsingWithUniqueNestedValues() throws Exception
    {
    	String url = "http://www1.macys.com/shop/product/calvin-klein-animal-print-faux-leather-inset-dress?ID=1668293&CategoryID=5449";
    	String configFile = "MicrodataFilterTest-uniqueNestedValues.json";
    	String contentFile = "macy.com_1668293.html";
    	Map<String, String[]> metadata = parse(url, configFile, contentFile);
        Assert.assertNotNull(metadata);
        System.out.println(KeyValues.toString(metadata, null));
        Assert.assertEquals("Women - Dresses",
            KeyValues.getValue("microdata.webpage.breadcrumb", metadata));
        Assert.assertEquals("Calvin Klein Animal-Print Faux-Leather-Inset Dress",
            KeyValues.getValue("microdata.product.name", metadata));
        Assert.assertEquals("1668293", KeyValues.getValue("microdata.product.productID", metadata));
        Assert.assertEquals(
            "http://slimages.macys.com/is/image/MCY/products/3/optimized/2338853_fpx.tif"
                    + "?wid=59&hei=72&fit=fit,1&$filtersm$",
            KeyValues.getValue("microdata.product.image", metadata));
        Assert.assertEquals("$74.99",
            KeyValues.getValue("microdata.product.offers.1.price", metadata));
        Assert.assertEquals("USD",
            KeyValues.getValue("microdata.product.offers.1.priceCurrency", metadata));
        Assert.assertEquals("http://schema.org/InStock",
            KeyValues.getValue("microdata.product.offers.1.availability", metadata));
    }

	private Map<String, String[]> parse(String url, String configFile, String contentFile) throws Exception {
		MicrodataFilter filter = prepareFilter(configFile);
		byte[] content = readContent(contentFile);
		DocumentFragment fragment = parseHtmlContent(content);

    	HashMap<String, String[]> metadata = new HashMap<>();
    	filter.filter(url, content, fragment, metadata);
    	return metadata;
	}

	private MicrodataFilter prepareFilter(String configFile) throws IOException {
		InputStream confStream = getClass().getClassLoader()
				.getResourceAsStream(configFile);
		ObjectMapper mapper = new ObjectMapper();
		JsonNode confNode = mapper.readValue(confStream, JsonNode.class);
		MicrodataFilter filter = new MicrodataFilter();
		filter.configure(new HashMap(), confNode);
		return filter;
    }

    private byte[] readContent(String filename) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(getClass().getClassLoader().getResourceAsStream(filename), baos);
        return baos.toByteArray();
    }

	private DocumentFragment parseHtmlContent(byte[] content) throws IOException, Exception {

        ByteArrayInputStream bais = new ByteArrayInputStream(content);

        InputSource inputSource = new InputSource(bais);

		DOMFragmentParser parser = new DOMFragmentParser();
		parser.setFeature("http://cyberneko.org/html/features/augmentations",
				true);
		parser.setProperty(
				"http://cyberneko.org/html/properties/default-encoding",
				"ISO-8859-1");
		parser.setFeature(
				"http://cyberneko.org/html/features/scanner/ignore-specified-charset",
				false);
		parser.setFeature(
				"http://cyberneko.org/html/features/balance-tags/ignore-outside-content",
				false);
		parser.setFeature(
				"http://cyberneko.org/html/features/balance-tags/document-fragment",
				true);

		HTMLDocumentImpl htmlDoc = new HTMLDocumentImpl();
		htmlDoc.setErrorChecking(false);
		DocumentFragment fragment = htmlDoc.createDocumentFragment();
		parser.parse(inputSource, fragment);
		return fragment;
	}
}
