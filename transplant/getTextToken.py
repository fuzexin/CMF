
"""
    generate the word_token.txt file
    it's general process is get all data's allnames, then use nltk 
    to delete the stopwords and get the stem words, finally, storage
    data in file. The file is used to train sen2vec.
"""


import logging, pickle
logging.basicConfig(level=logging.INFO)

import os,argparse

from nltk.tokenize import  word_tokenize
from nltk.stem import PorterStemmer
import string

# ---------------------utility function--------------------------------------
def process_texts(texts):
    ''' process texts list '''
    l = []
    i = 0
    for t in texts:
        l.append(tokenize_pipeline2(t))
        i += 1
        if i % 1000 == 0:
            logging.info("has processed {} text".format(i))
    return l

def tokenize_pipeline2(text):
    stop_words = get_stopwords_basic()
    tokens = word_tokenize(text)
    tokens = [w.lower() for w in tokens]
    words = tokens
    stripped = [w.translate(string.punctuation) for w in tokens]
    words = [word for word in stripped if word.isalpha() and is_english(word)]
    words = [w for w in words if not w in stop_words]
    words = nltk_stem(words)  # nltk_lemmatize
    words = [w for w in words if not w in stop_words] # final remove
    return words  

def get_stopwords_basic():
    file = '/nfs/home/fzx/project/CMF/code/data/stopwords-en-basic.txt'
    assert check_exist(file), "can not find stopwords file {}".format(file)
    return open(file).read().split('\n')

def check_exist(outf):
    return os.path.isfile(outf)

def is_english(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True

def nltk_stem(word_l):
    return [PorterStemmer().stem(i) for i in word_l]

# ----------------------------------------------------------------------------
# get data
def getWord_Token(data, deposit_dir):
    summary_list = set()
    # 2.get every data’s AllNames
    for one_data in data:
        allnames = one_data[4]
        if len(allnames) > 10:
            summary_list.add(one_data[4])
    
    summary_list = list(summary_list)
    # 3.delet stopwords, other similar operation;
    summary_list = process_texts(summary_list)

    # 4.get the name and Embedding vector;
    text_f = os.path.join(deposit_dir, 'text_token.txt')
    outf = open(text_f, 'w')
    for sent in summary_list:
        sent = ' '.join(sent)
        outf.write("{}\n".format(sent))
    outf.close()
    logging.info(text_f, 'saved')
     

if __name__ == "__main__":

    parser = argparse.ArgumentParser(usage="to get the text_token.txt file")
    parser.add_argument('-d',required=True)
    parser.add_argument('-p',required=True)
    args = parser.parse_args()
    # there are more than 1 data file in which one represent one location
    # the data directory
    data_dir = args.d
    files = os.listdir(data_dir)
    data = []
    for one_file in files:
        file_path = os.path.join(data_dir, one_file)
        with open(file_path, 'rb') as f:
            file_data = pickle.load(f)
            data.extend(file_data)
    logging.info(f"total data num: {len(data)}")
    # where to deposit the text_token.txt file
    deposit_dir = args.p
    getWord_Token(data, deposit_dir)



    