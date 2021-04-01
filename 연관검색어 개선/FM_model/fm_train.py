import numpy as np
import xlearn as xl
import pandas as pd
import nmslib
import json
import pickle
import os
import csv


class LabelConverter(object):
    def __init__(self, value_to_label, label_to_value, start_label):
        self.value_to_label = value_to_label
        self.label_to_value = label_to_value
        self.start_label = start_label

    def size(self):
        return len(self.value_to_label)

    def from_value(self, value):
        if value in self.value_to_label:
            return self.value_to_label.at[value]
        return None

    def from_label(self, label):
        if label in self.label_to_value:
            return self.label_to_value.at[label]
        return None


class FM(object):
    def __init__(self, dataset_path, output_path):
        self._dataset_path = dataset_path
        self._output_path = output_path
        self._txt_model_file = 'fm_model.txt'
        self._out_model_file = 'fm_model.bin'
        self._num_dim = 100
        self._num_result = 30  # the number of related keyword
        self._keyword_label_converter = None
        self._prev_keyword_label_converter = None
        self._next_keyword_label_converter = None
        self._hnsw_index = None
        self._knn = None
        self.result_files = None

    def train(self):
        self._read_labels()
        self._build_model()
        self._keyword_vectors = self._read_fm_vectors()
        self._hnsw_index = self._build_hnsw_index_nmslib()
        self._knn = self._build_knn()
        self._build_result(self._knn)

    def _read_labels(self):
        print('Read keyword label')
        self._keyword_label_converter = self._get_label_converter(self._dataset_path + '/keyword_label.csv', 0)
        print('Read prev keyword label')
        self._prev_keyword_label_converter = self._get_label_converter(
            self._dataset_path + '/prev_keyword_label.csv',
            self._keyword_label_converter.size())
        print('Read next keyword label')
        self._next_keyword_label_converter = self._get_label_converter(
            self._dataset_path + '/next_keyword_label.csv',
            self._keyword_label_converter.size() + self._prev_keyword_label_converter.size()
        )

    def _get_label_converter(self, file, start_label=0):
        label_file = file
        labels = pd.read_csv(label_file, dtype=str)
        print(f'shape: {labels.shape}')
        content_to_label, label_to_content = self._get_label_map(labels, start_label)
        return LabelConverter(content_to_label, label_to_content, start_label)

    def _get_label_map(self, labels: pd.DataFrame, start_label=0):
        label_array = (labels.iloc[:, 1].astype('int') + start_label).array
        value_array = labels.iloc[:, 0].astype('str').array
        label_to_value = pd.Series(value_array, index=label_array)
        value_to_label = pd.Series(label_array, index=value_array)
        return value_to_label, label_to_value

    def _build_model(self):
        print("Build Model")
        training_file = self._dataset_path + '/fm_libsvm.csv'

        fm_model = xl.create_fm()
        fm_model.setTrain(training_file)

        param = {
            'task': 'reg',
            'lr': 0.15,
            'epoch': 10,
            'lambda': 0.002,
            'metric': 'rmse',
            'nthread': os.cpu_count(),
            'k': self._num_dim
        }

        with open('fm_parameter.json', 'w') as file:
            json.dump(param, file, indent=4)

        fm_model.setTXTModel(self._txt_model_file)
        fm_model.fit(param, self._out_model_file)

    def _read_fm_vectors(self):
        print('Read vectors from model')
        _keyword_vectors = [None] * self._keyword_label_converter.size()
        print(f'Keyword label size: {len(_keyword_vectors)}')

        with open(self._txt_model_file, 'r') as file:
            for line in file:
                if not line.startswith('v_'):
                    continue

                fields = [x.strip() for x in line.split(':')]
                keyword_label = int(fields[0][2:])
                if keyword_label % 1000000 == 0:
                    print(f'Reading vectors: {keyword_label}')
                if keyword_label in self._keyword_label_converter.label_to_value:
                    label = keyword_label - self._keyword_label_converter.start_label
                    _keyword_vectors[label] = np.fromstring(fields[1], dtype=float, sep=' ')

            print(f'Reading vecotrs: {keyword_label}')
        with open(self._output_path + '/fm_vectors.pkl', 'wb') as f:
            pickle.dump(_keyword_vectors, f)

        return _keyword_vectors

    def _build_hnsw_index_nmslib(self):
        print('Build HNSW Index')
        hnsw_index = nmslib.init(method='hnsw', space='cosinesimil')
        hnsw_index.addDataPointBatch(self._keyword_vectors)

        params = {
            'M': 16,
            'efConstruction': 100,
            'post': 0,
            'indexThreadQty': os.cpu_count()
        }

        hnsw_index.createIndex(params, print_progress=True)
        hnsw_index.saveIndex(self._output_path + '/rfm_knn_index_nmslib.bin', save_data=False)
        return hnsw_index

    def _build_knn(self):
        print('Find nearest neighbors')
        neighbors = self._hnsw_index.knnQueryBatch(self._keyword_vectors, k=self._num_result + 1,
                                                   num_threads=os.cpu_count())
        return neighbors

    def _build_result(self, knn):
        candidates = [[source_label, np.array(target_labels), np.array(distances)]
                      for source_label, (target_labels, distances) in enumerate(knn)]

        output_file = self._output_path + '/fm_result.csv'
        with open(output_file, 'w') as file:
            writer = csv.writer(file, quoting=csv.QUOTE_ALL, lineterminator='\n')
            for source_label, target_labels, distances in candidates:
                source_keyword = self._keyword_label_converter.from_label(source_label)
                if source_keyword is None:
                    continue

                target_keywords = [self._keyword_label_converter.from_label(x) for x in target_labels]
                similar_keywords = [[target_keyword, distance] for target_keyword, distance in
                                    zip(target_keywords, distances)
                                    if target_keyword != source_keyword]
                if not similar_keywords:
                    continue

                value = '|'.join([f'{target}:{score:.5f}' for target, score in similar_keywords[:self._num_result]])
                writer.writerow([source_keyword, value])

        print('Complete writing results')


dataset_path = './data'
output_path = './output'
model = FM(dataset_path, output_path)
model.train()
