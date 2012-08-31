some quick hacks using the <a href="http://commoncrawl.org/">common crawl dataset</a>

<a href="common-crawl-quick-hacks/tree/master/links_in_metadata">links in metadata</a> is an example of using hadoop streaming with a python script to extract links from the metadata set

<a href="common-crawl-quick-hacks/tree/master/finding_names">finding names</a> gives a quick overview of the textdata set and presents a simple NLTK app for extracting noun phrases
(again python streaming)

<a href="common-crawl-quick-hacks/tree/master/url_status_codes">url status codes</a> shows how to run over the metadata set using java mapreduce to extract urls and the status codes
the crawler received when crawling them