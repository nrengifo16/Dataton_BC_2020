import dask.dataframe as dd
import pandas as pd
import numpy as np

def calculate_mape(y, h_hat):
    return np.mean(abs((y - y_hat)/y))

def absolute_error(y, y_hat):
    return abs((y - y_hat)/y)


if __name__ == '__main__':
    file_real = pd.read_csv('data/train_set_500_cleaned.csv')
    file_pred = pd.read_csv('data/our_predictions.csv')

    y     = file_real['gasto_familiar']
    y_hat = file_pred['gasto_familiar']

    mape = calculate_mape(y, y_hat)
    print(mape)
