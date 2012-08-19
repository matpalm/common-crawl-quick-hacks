# common crawl textData

the central offering from common crawl is the raw bytes downloaded as part of their crawl. this is useful for some people but for a lot
of users they are after just web pages visible text. luckily they've done this extraction as a part of post processing the crawl 
and it's freely available too!

## getting the data

the first thing we need to get some is determine which segments of the crawl are valid and ready for use (the crawl is always ongoing)

    s3cmd get s3://aws-publicdatasets/common-crawl/parse-output/valid_segments.txt
    head -n3 valid_segments.txt
    1341690147253
    1341690148298
    1341690149519

we can then use this to get the textData objects

if you just want one grab it simply like...

    s3cmd ls s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/ 2>/dev/null \
     | grep textData | head -n1 | awk '{print $4}'
    s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/textData-00000

but if you want the lot (and are prepared to wait an hour) you can get the lot with

    cat valid_segments.txt \
     | xargs -I{} s3cmd ls s3://aws-publicdatasets/common-crawl/parse-output/segment/{}/ \
     | grep textData | awk '{print $4}' > all_valid_segments.tsv

this equates to 200,000 of textData files totally over 6TB. each textData file is a hadoop sequence files, the key being the crawled url and the value being the extracted visible text. 

to have quick look at one...

    hadoop fs -text textData-00000 | less

and you'll see examples like

    http://webprofessionals.org/intel-to-acquire-mcafee-moving-into-online-security-ny-times/       Web Professionals 
    Professional association for web designers, developers, marketers, analysts and other web professionals.
    Home 
    ...
    The company’s share price has fallen about 20 percent in the last five years, closing on Wednesday at $19.59 a share.
    Intel, however, has been bulking up its software arsenal. Last year, it bought Wind River for $884 million, giving it a software maker with a presence in the consumer electronics and wireless markets.
    With McAfee, Intel will take hold of a company that sells antivirus software to consumers and businesses and a suite of more sophisticated security products and services aimed at corporations.

( note that the visible text is broken into <i>one line</i> per block element from the original html. as such the value in the key/value pairs includes carriage returns and, for something like less, gets
outputted as being seperate lines )

## extracting noun phrases

now that we have some text, what can we do with it? one thing is to look for noun phrases and the quickest simplest way is to use something like 
the python natural language toolkit; <a href="http://nltk.org/">nltk</a>. it's certainly not the fastest to run but for most people would be
the quickest to get going.

we can use something like this <a href="">extract_noun_phrases.py</a> script to extract the noun phrases from the text.

eg for the text ...

    Last year, Microsoft bought Wind River for $884 million. This makes it the largest software maker with a presence in North Kanada.

we extract noun phrases like ...

    Microsoft
    Wind River
    North Kanada

and to run this at larger scale we can wrap it in a simple streaming job

    hadoop jar $HADOOP_HOME/hadoop-streaming.jar \
     -input textDataFiles \
     -output counts \
     -mapper extract_noun_phrases.py \
     -reducer aggregate \
     -file extract_noun_phrases.py

run across a small 50mb sample of textData files the top noun phrases are...
  
so it's fun to look at noun phrases but i've actually brushed over some key details

* not filtering on english text first generates a <i>lot</i> of "noise". "G úûv ÝT M", "U ŠDú T" and "Y CKdñˆô" are not terribly interesting english noun phrases.
* running this at scale you'd probably want to change off streaming and start using an in process java library like <a href="http://nlp.stanford.edu/software/lex-parser.shtml">the stanford parser</a>
* when it comes to actually doing named entity recognition it's a bit more complex. there's <a href="http://blog.wavii.com/2012/08/16/bush-is-back/">a wavii blog post</a> from <a href="https://twitter.com/mkbubba">manish</a> that talks a bit more about it. 
