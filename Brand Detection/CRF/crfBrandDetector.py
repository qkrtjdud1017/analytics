from sklearn_crfsuite import metrics, CRF
from sklearn.model_selection import train_test_split
import pickle
import numpy as np
import pandas as pd


class CrfBrandDetector:
    def __init__(self):
        self.df = None
        self.test_index = None
        self.crf = None
        self.y_pred = None

    def train_test_split(self, df, test_size=0.2, random_state=123):
        self.df = df
        x_train, x_test, y_train, y_test = train_test_split(
            self.df['features'], self.df['labels'],
            test_size=test_size,
            random_state=random_state
        )
        self.test_index = x_test.index
        return x_train, x_test, y_train, y_test

    def fit(self, x_train, y_train):
        self.crf = CRF(
            algorithm='lbfgs',  # Gradient descent using the L-BFGS method
            c1=0.05,  # The coefficient for L1 regularization
            c2=0.05,  # The coefficient for L2 regularization
            max_iterations=100,
            all_possible_states=True
            # When True, CRFsuite generates state features that associate all of possible combinations between attributes and labels.
        )
        self.crf.fit(x_train, y_train)

    def save_model(self, model_file_name='crf_model.sav'):
        pickle.dump(self.crf, open(model_file_name, 'wb'))

    def predict(self, x):
        title = [[diction['word_lower'] for diction in obs] for obs in x]
        prediction = self.crf.predict(x)
        idx = [[True if label == '1' else False for label in obs] for obs in prediction]
        preds = [' '.join(np.array(title[i])[idx[i]]) for i in range(len(title))]
        df_pred = pd.concat([
            self.df[self.df.index.isin(self.test_index)].reindex(self.test_index).reset_index().origin_title,
            pd.DataFrame(preds)
        ], axis=1)
        df_pred.columns = ['title', 'predicted_brand']
        df_pred['predicted_brand'] = df_pred.apply(
            lambda row: row.predicted_brand if row.predicted_brand in row.title.lower()
            else row.predicted_brand.split(), axis=1
        )
        return df_pred

    def report_classification(self, x_test, y_test):
        labels = list(self.crf.classes_)
        labels.remove('0')
        self.y_pred = self.crf.predict(x_test)
        print(metrics.flat_classification_report(
            y_test,
            self.y_pred,
            labels=labels,
            digits=3
        ))

    def evaluate(self, x_test, y_test):
        acc = float(list(y_test == self.y_pred).count(True)) / len(self.y_pred)
        return acc
