import pandas as pd
from sqlalchemy import create_engine
import re
import os
import sent2vec
import nmslib
import csv
import numpy as np

q = '''
    SELECT id,
           name,
           keyword
    FROM service1_quicket.product_info
    WHERE category_id = 700600400
      AND status IN (0, 1)
'''

df = pd.read_sql(q, con=bun_dw)
print('Shape of the dataframe:', df.shape)

# male1 = '\'%' + '남성' + '\'%'
# male2 = '\'%' + '남자' + '\'%'
# female1 = '\'%' + '여성' + '\'%'
# female2 = '\'%' + '여자' + '\'%'
#
# q = f'''
#     SELECT id,
#            name,
#            keyword
#     FROM service1_quicket.product_info
#     WHERE category_id = 700600400
#       AND status IN (0, 1)
#       AND (name NOT LIKE {male1}
#            OR keyword NOT LIKE {male1}
#            OR name NOT LIKE {male2}
#            OR keyword NOT LIKE {male2}
#            OR name NOT LIKE {female1}
#            OR keyword NOT LIKE {female1}
#            OR name NOT LIKE {female2}
#            OR keyword NOT LIKE {female2})
# '''
# print(q)
q = '''
    SELECT *
    FROM workspace.golf_category
'''
train_df = pd.read_sql(q, con=bun_dw)


def preprocess_product_name(text):
    text = str(text)
    text = re.sub(r"[^가-힣ㄱ-ㅎㅏ-ㅣA-z0-9-:/!@#$%^?&*()_=+.~{}\[\]]|\\", " ", text)
    text = re.sub(r'\s+', ' ', text)
    return text.lower().strip()

df['name'] = df['name'].map(preprocess_product_name)
df['keyword'] = df['keyword'].map(preprocess_product_name)
df.drop_duplicates('name', inplace=True)
product_names = df['name'].values.tolist()

train_df['name'] = train_df['name'].map(preprocess_product_name)
train_df['keyword'] = train_df['keyword'].map(preprocess_product_name)
print('Before drop duplicates:', train_df.shape)
train_df.drop_duplicates('name', inplace=True)
print('After drop duplicates:', train_df.shape)
train_product_names = train_df['name'].values.tolist()
# train_product_names = train_df['name'].map(lambda x: str(x) + ' ').values + train_df['keyword'].values
train_product_keywords = train_df['keyword'].values.tolist()

train_set = df['name'].map(lambda x: str(x) + ' ').values + df['keyword'].values

product_name_list = 'products_for_pretrain.txt'
with open(product_name_list, 'w') as file:
    file.writelines(map(lambda x: str(x) + '\n', train_set))

sent2vec_model_file = 'sent2vec_model.bin'
sent2vec_index_file = 'sent2vec_index.bin'
sent2vec_lib_path = './sent2vec'

param = {
    "input": product_name_list,
    "output": sent2vec_model_file.split('.')[0],
    "minCount": 3,
    "dim": 100,
    "epoch": 10,
    "lr": 0.2,
    "wordNgrams": 3,
    "loss": "ns",
    "neg": 10,
    "thread": os.cpu_count(),
    "t": "0.000005",
    "dropoutK": 4,
    "minCountLabel": 20,
    "bucket": 500000,
    "maxVocabSize": 100000,
    "numCheckPoints": 0,
    "verbose": 1
}
parse_param = " ".join(["-{} {}".format(k, v) for k, v in param.items()])

cmd = f"{sent2vec_lib_path}/fasttext sent2vec "
cmd = cmd + parse_param
os.system(cmd)

model = sent2vec.Sent2vecModel()
model.load_model(sent2vec_model_file)

p_name_vec = model.embed_sentences(train_product_names)
indices = []
product_name_vec_non_zero = []
for i, v in enumerate(p_name_vec):
    if v.sum() != 0:
        indices.append(i)
        product_name_vec_non_zero.append(v)
products_non_zero = train_df.iloc[indices, :].reset_index(drop=True)
num_to_content_id = products_non_zero.id.to_dict()
num_to_product_name = products_non_zero.name.to_dict()
num_to_product_keyword = products_non_zero.keyword.to_dict()

keywords = ['남성', '여성']
keywords_non_zero = []
keywords_vec_non_zero = []
keywords_vec = model.embed_sentences(keywords)
for i, v in enumerate(keywords_vec):
    if v.sum() != 0:
        keywords_non_zero.append(keywords[i])
        keywords_vec_non_zero.append(v)


hnsw_index = nmslib.init(method='hnsw', space='cosinesimil')
hnsw_index.addDataPointBatch(product_name_vec_non_zero)

# post parameter: https://github.com/nmslib/nmslib/issues/121
params = {
    'M': 32,
    'efConstruction': 250,
    'post': 0
}

hnsw_index.createIndex(params, print_progress=True)
hnsw_index.saveIndex(sent2vec_index_file, save_data=True)

neighbors = hnsw_index.knnQueryBatch(
    keywords_vec_non_zero,
    k=1000,
    num_threads=os.cpu_count()
)

output_file = './result_onlyname.csv'
num_zeros = 0
with open(output_file, 'w') as file:
    writer = csv.writer(file, quoting=csv.QUOTE_ALL)
    for source_label, (target_labels, distances) in enumerate(neighbors):
        keyword = keywords_non_zero[source_label]
        content_ids = list(map(lambda x: num_to_content_id[x], target_labels))
        names = list(map(lambda x: num_to_product_name[x], target_labels))
        tags = list(map(lambda x: num_to_product_keyword[x], target_labels))
        num_zeros += (len(distances) - np.count_nonzero(distances))

        # value = '\n'.join(
        #     [f"{name}|{tag}:{distance:.5f}"
        #      for name, tag, distance in list(zip(names, tags, distances))]
        # )
        value = '\n'.join(
            [f"{id} {name} : {distance:.5f}"
             for id, name, distance in list(zip(content_ids, tags, distances))]
        )
        writer.writerow([keyword, value])
