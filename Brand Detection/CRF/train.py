from dataLoader import DataLoader
from crfBrandDetector import CrfBrandDetector

if __name__ == '__main__':
    print('Preparing Data...')
    df = DataLoader().get_data()
    print('Building Model...')
    crf_model = CrfBrandDetector()
    print('Fitting...')
    x_train, x_test, y_train, y_test = crf_model.train_test_split(df)
    crf_model.fit(x_train, y_train)
    crf_model.report_classification(x_test, y_test)
    print('Accuracy: {}'.format(crf_model.evaluate(x_test, y_test)))
    pred = crf_model.predict(x_test)
    pred.to_csv('./pred.csv', index=False)
