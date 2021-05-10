import string
from nltk.tokenize import word_tokenize
import pandas as pd


def labeling(label, string):
    return [(word, label) for word in word_tokenize(string)]


def branding(df):
    t, br = df.title, df.brand
    start = (t.lower()).find(br.lower())
    end = start + len(br)
    labeled_title = labeling('0', t[:start]) + labeling('1', t[start:end]) + labeling('0', t[end:])
    return labeled_title


def word2features(row, i):
    word = str(row.title[i][0])
    features = {
        'root_cat': row.root_cat,
        'bias': 1,
        'word_position': i,
        'word_lower': word.lower(),
        'word[-2:]': word[-2:],
        'word_isUpper': word.isupper(),
        'word_isTitle': word.istitle(),
        'word_isDigit': word.isdigit(),
    }

    if i > 0:
        prev_word = str(row.title[i - 1][0])
        features.update({
            'prev_word_lower': prev_word.lower(),
            'prev_word_isTitle': prev_word.istitle(),
            'prev_word_isUpper': prev_word.isupper(),
        })
    else:
        features['BOS'] = True

    if i < len(row.title) - 1:
        next_word = str(row.title[i + 1][0])
        features.update({
            'next_word_lower': next_word.lower(),
            'next_word_isTitle': next_word.istitle(),
            'next_word_isUpper': next_word.isupper(),
            'next_word_anyDigit': any(ch.isdigit() for ch in next_word),
            'next_word_isPuctuation': next_word in string.punctuation,
        })
    else:
        features['EOS'] = True

    if i > 1:
        prev1 = str(row.title[i - 1][0])
        prev2 = str(row.title[i - 2][0])
        features.update({
            '-2ngram': '{} {}'.format(prev1, prev2)
        })

    if i < len(row.title) - 2:
        next1 = str(row.title[i + 1][0])
        next2 = str(row.title[i + 2][0])
        features.update({
            '+2ngram': '{} {}'.format(next1, next2)
        })

    return features


class DataLoader:
    """
    data from eBay
    columns:
    product title, brand, root_cat (root category of item)
    """

    def get_data(self, file_name='../data/train.csv'):
        df = pd.read_csv(file_name)
        df['origin_title'] = df['title'].values
        df['title'] = df.apply(branding, axis=1)
        df['features'] = df.apply(lambda row: [word2features(row, i)
                                               for i in range(len(row.title))], axis=1)
        df['labels'] = df.apply(lambda row: [label for token, label in row.title], axis=1)

        return df
