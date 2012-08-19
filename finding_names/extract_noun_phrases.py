#!/usr/bin/env python
import sys, string, re, codecs, locale
from nltk import sent_tokenize, word_tokenize, pos_tag, RegexpParser
from nltk.tokenize import WordPunctTokenizer, PunktWordTokenizer, PunktSentenceTokenizer

#grammar = "NP: {<JJ>*<NNP>+}"  # 0 or more adjectives followed by 1 or more noun phrases)
grammar = "NP: {<NNP>+}" # 1 or more noun phrases

chunk_parser = RegexpParser(grammar)

sent_tokenizer = PunktSentenceTokenizer()

term_tokenizer = WordPunctTokenizer()  # gut feel this is better, see compare_tokenizers.sh in section stuff
#term_tokenizer = PunktWordTokenizer()

sys.stdin = codecs.getreader(locale.getpreferredencoding())(sys.stdin)
sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
sys.stderr = codecs.getwriter(locale.getpreferredencoding())(sys.stderr)

def alpha_numeric(t): 
    return t.isdigit() or (t.lower() >= 'a' and t.lower() <= 'z')

def at_least_one_alphanumeric(word):
    for char in word:
        if alpha_numeric(char):
            return True
    return False

def remove_trailing_period(token):
    return re.sub(r'\.$', '', token)


# some counters
num_lines_too_short = 0
num_sentences = 0
num_exceptions = 0
num_noun_phrases_too_long = 0

for text in sys.stdin:
    try:
        text = text.strip()

        # ignore short lines they are probably visible text extraction noise
        if len(text) < 10:
            num_lines_too_short += 1
            continue

        # split into sentences
        for sentence in sent_tokenizer.tokenize(text):
            num_sentences += 1

            # split sentence into tokens
            tokens = term_tokenizer.tokenize(sentence)
            tokens_without_trailing_periods = map(remove_trailing_period, tokens)  # required for PunktWordTokenizer

            # pos tag sentence
            tagged = pos_tag(tokens_without_trailing_periods)
            if not len(tagged)==0:
                parse_tree = chunk_parser.parse(tagged)
                for subtree in parse_tree.subtrees():
                    if subtree.node == 'NP':
                        phrase = subtree.leaves()
                        noun_phrase = [term for (term, pos_type) in phrase] 
                        if len(noun_phrase) > 10:
                            # occasional weird parse
                            num_noun_phrases_too_long += 1
                        else:
                            # clean up a bit and emit anything left that seems sensible
                            noun_phrase = " ".join(filter(at_least_one_alphanumeric, noun_phrase))
                            if len(noun_phrase) > 3:
                                print "LongValueSum:%s\t1" % noun_phrase

    except Exception, e:
        num_exceptions += 1
        sys.stderr.write("error! on line ["+text+"] ["+str(e)+"]\n")

# counters
print >>sys.stderr, "reporter:counter:stats,num_lines_too_short,%d" % num_lines_too_short
print >>sys.stderr, "reporter:counter:stats,num_sentences,%d" % num_sentences
print >>sys.stderr, "reporter:counter:stats,num_exceptions,%d" % num_exceptions
print >>sys.stderr, "reporter:counter:stats,num_noun_phrases_too_long,%d" % num_noun_phrases_too_long



