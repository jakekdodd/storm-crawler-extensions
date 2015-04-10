JSoup HTML Parser
========================

This project is deprecated. Please use https://github.com/DigitalPebble/storm-crawler instead.

This is a Bolt similar to the ParserBolt. The difference being that 
it uses JSoup to do the HTML Parsing instead of Tika.

# How to add it to your project?

This project is published as a maven artifact, so all you need to do is
add it as a dependency to your own project:

```
<dependency>
    <groupId>com.shopstyle.storm-crawler-extensions</groupId>
    <artifactId>jsoup-parser</artifactId>
    <version>0.1</version>
</dependency>
```

# How to build it?

This project uses [gradle](http://gradle.org/). Run `gradle build` to
build the jar locally. To create the associated Eclipse project, run 
`gradle eclipse`.
