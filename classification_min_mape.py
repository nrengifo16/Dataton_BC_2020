import dask.dataframe as dd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from mape import absolute_error
np.set_printoptions(suppress=True)

def adjusted_coefficient(R2, n, k):
    coefficient = 1 - ((1 - R2)*(n - 1)/(n - k - 1))
    return coefficient

def maxk(array, k):
    idxs_if_sorted = np.argsort(array)
    idxs_of_interest = idxs_if_sorted[-1:-1*k-1:-1]
    vals_of_interest = array[idxs_of_interest]

    return idxs_of_interest, vals_of_interest

df = dd.read_csv('data/train_set_1m_cleaned.csv')

# Variables to select
name_selected_variables = [
    'edad', 
    'genero', 
    'categoria', 
    'ind_mora_vigente', 
    'cartera_castigada', 
    'cant_moras_30_ult_12_meses', 
    'cant_moras_60_ult_12_meses', 
    'cant_moras_90_ult_12_meses',
    'cupo_total_tc',
    'tenencia_tc',
    'cuota_tc_bancolombia',
    "tiene_consumo",
    "tiene_crediagil",
    'nro_tot_cuentas',
    'ctas_activas',
    "tiene_ctas_activas",
    "ctas_embargadas",
    "tiene_ctas_embargadas",
    "pension_fopep",
    "cuota_cred_hipot",
    "tiene_cred_hipo_1",
    "tiene_cred_hipo_2",
    "mediana_nom3",
    "mediana_pen3",
    "ingreso_nompen",
    "ingreso_final",
    "cant_mora_30_tdc_ult_3m_sf",
    "cant_mora_30_consum_ult_3m_sf",
    "cuota_de_vivienda",
    "cuota_de_consumo",
    "cuota_rotativos",
    "cuota_tarjeta_de_credito",
    "cuota_de_sector_solidario",
    "cuota_sector_real_comercio",
    "cupo_tc_mdo",
    "saldo_prom3_tdc_mdo",
    "cuota_tc_mdo",
    "saldo_no_rot_mdo",
    "cuota_libranza_sf",
    "cant_oblig_tot_sf",
    "cant_cast_ult_12m_sr",
    "ind",
    "pol_centr_ext",
    "ingreso_nomina",
    "ingreso_segurida_social",
    "gasto_familiar"
]
df      = df[name_selected_variables]

X       = np.array(df[name_selected_variables[:-1]])
new_col = np.ones((df.shape[0].compute(),1))

X       = np.hstack((X,new_col))
X[:,-1] = np.array(list(range(df.shape[0].compute())))
y       = np.array(df[name_selected_variables[-1]])

file_index = 0
parameters = pd.DataFrame(columns=range(1,47))

min_group_size         = 12000
num_elements_to_remove = 6000

while (X.shape[0] > min_group_size):
    new_X = X
    new_y = y

    i            = 1
    max_R2       = 0
    optimal_X    = None
    optimal_y    = None
    optimal_reg  = None
    coefficients = []

    while (new_X.shape[0] > min_group_size):
        reg         = LinearRegression().fit(new_X[:,0:-1], new_y)
        predictions = reg.predict(new_X[:,0:-1])
        mape        = absolute_error(np.array(new_y), np.array(predictions))

        ind, vals   = maxk(np.array(mape), num_elements_to_remove)

        R2_adjusted = adjusted_coefficient(reg.score(new_X[:,0:-1],new_y), new_X.shape[0], len(name_selected_variables) - 1)

        if R2_adjusted > max_R2:
            max_R2      = R2_adjusted
            optimal_X   = new_X
            optimal_y   = new_y
            optimal_reg = reg

        #     print(i*5000, ' ', new_X.shape[0])
        #     i += 1
        new_X = np.delete(new_X, ind, 0)
        new_y = np.delete(new_y, ind, 0)
        
        coefficients.append(R2_adjusted)

        ind = []

    # plt.plot(coefficients)
    # plt.show()
    # print(max_R2)

    np.savetxt(f'classes/group_{file_index}.csv', optimal_X, delimiter=',')

    # Save the optimal model parameters
    parameters.loc[file_index] = [optimal_reg.intercept_] + list(optimal_reg.coef_)

    true_index = optimal_X[:,-1]
    bool_mask  = [not (x1 in true_index) for x1 in X[:,-1]]

    X = X[bool_mask]
    y = y[bool_mask]

    print(f'Group {file_index} created')
    file_index += 1

np.savetxt(f'classes/group_{file_index}.csv', X, delimiter=',')

reg         = LinearRegression().fit(X[:,0:-1], y)
R2_adjusted = adjusted_coefficient(reg.score(X[:,0:-1],y), X.shape[0], len(name_selected_variables) - 1)

parameters.loc[file_index] = [reg.intercept_] + list(reg.coef_)
parameters.to_csv('parameters/parameters.csv', index=False)