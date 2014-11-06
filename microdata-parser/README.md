Microdata Parsing Filter
========================

This is a ParseFilter to be plugged in with the ParserBolt. It extracts
all the microdata stored in the page and adds it to the metadata map
using a key-dotted path. 

To add it the list of filters, simply add the following to the list:
```
    {
      "class": "com.shopstyle.crawler.microdata.MicrodataFilter",
      "name": "Microdata"
    }
```

There are 2 modes toggled via a boolean parameter `useUniquePrefixForNestedValues`.
When set to `true`, nested values get a numerical prefix. The default 
is to merge all of the values within a single entry instead. See the 
unit tests for an example.

# How to add it to your project?

This project is published as a maven artifact, so all you need to do is
add it as a dependency to your own project:

```
<dependency>
    <groupId>com.shopstyle.storm-crawler-extensions</groupId>
    <artifactId>microdata-parser</artifactId>
    <version>0.2</version>
</dependency>
```

# How to build it?

This project uses [gradle](http://gradle.org/). Run `gradle build` to
build the jar locally. To create the associated Eclipse project, run 
`gradle eclipse`.
