import numpy as np
import pandas as pd
from sklearn.linear_model import RidgeCV, LassoCV
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import StackingRegressor
from sklearn.svm import LinearSVR
from sklearn.ensemble import RandomForestRegressor

variables= ['edad', 'genero', 'categoria', 'ind_mora_vigente', 'cartera_castigada', 'cant_moras_30_ult_12_meses', 'cant_moras_60_ult_12_meses', 'cant_moras_90_ult_12_meses','cupo_total_tc','tenencia_tc','cuota_tc_bancolombia',"tiene_consumo","tiene_crediagil",'nro_tot_cuentas','ctas_activas',"tiene_ctas_activas","ctas_embargadas","tiene_ctas_embargadas","pension_fopep","cuota_cred_hipot","tiene_cred_hipo_1","tiene_cred_hipo_2","mediana_nom3","mediana_pen3","ingreso_nompen","ingreso_final","cant_mora_30_tdc_ult_3m_sf","cant_mora_30_consum_ult_3m_sf","cuota_de_vivienda","cuota_de_consumo","cuota_rotativos","cuota_tarjeta_de_credito","cuota_de_sector_solidario","cuota_sector_real_comercio","cupo_tc_mdo","saldo_prom3_tdc_mdo","cuota_tc_mdo","saldo_no_rot_mdo","cuota_libranza_sf","cant_oblig_tot_sf","cant_cast_ult_12m_sr","ind","pol_centr_ext","ingreso_nomina","ingreso_segurida_social"]

train_data = pd.read_csv('data/train_set_500_cleaned.csv')
train_data = train_data.fillna(-1)
test_data  = pd.read_csv('data/test_set_cleaned.csv')
test_data  = test_data.fillna(-1)
print("Cargue datos")

X_train = np.array(train_data[variables])
y_train = np.array(train_data.iloc[:, -1])

X_test  = test_data[variables]

estimators = [
    ('ridge', RidgeCV()),
    ('lasso', LassoCV(random_state=42)),
    ('knr', KNeighborsRegressor(n_neighbors=20, metric='euclidean')),
    ('Random Forest', RandomForestRegressor(criterion="mae"))]

final_estimator = GradientBoostingRegressor(n_estimators=25, subsample=0.5, min_samples_leaf=20, max_features=1,random_state=42)
reg = StackingRegressor(estimators=estimators,final_estimator=final_estimator)

reg.fit(X_train, y_train)
print("Entrene")

y_hat = reg.predict(X_test)

df          = pd.DataFrame(columns=['id_registro', 'gasto_familiar'])
id_registro = list(test_data.id_registro)

df['id_registro']    = id_registro
df['gasto_familiar'] = y_hat

df.to_csv('predictions.csv', index=False)