import pandas as pd
import scipy as sp
import numpy as np
import dask.dataframe as dd
import scipy.stats as st

def read_groups(file_name):
    df         = pd.read_csv(file_name)
    df= df.iloc[:,:-1] ##voy a leer todas menos la ultima columna
    header= ['edad', 'genero', 'categoria', 'ind_mora_vigente', 'cartera_castigada', 'cant_moras_30_ult_12_meses', 'cant_moras_60_ult_12_meses', 'cant_moras_90_ult_12_meses','cupo_total_tc','tenencia_tc','cuota_tc_bancolombia',"tiene_consumo","tiene_crediagil",'nro_tot_cuentas','ctas_activas',"tiene_ctas_activas","ctas_embargadas","tiene_ctas_embargadas","pension_fopep","cuota_cred_hipot","tiene_cred_hipo_1","tiene_cred_hipo_2","mediana_nom3","mediana_pen3","ingreso_nompen","ingreso_final","cant_mora_30_tdc_ult_3m_sf","cant_mora_30_consum_ult_3m_sf","cuota_de_vivienda","cuota_de_consumo","cuota_rotativos","cuota_tarjeta_de_credito","cuota_de_sector_solidario","cuota_sector_real_comercio","cupo_tc_mdo","saldo_prom3_tdc_mdo","cuota_tc_mdo","saldo_no_rot_mdo","cuota_libranza_sf","cant_oblig_tot_sf","cant_cast_ult_12m_sr","ind","pol_centr_ext","ingreso_nomina","ingreso_segurida_social"]
    df.columns = header
    return df

def data_frames_creator():
    all_dataframes=[]
    for i in range(52):
        dataG=read_groups("data/classes/group_"+str(i)+".csv")
        all_dataframes.append(dataG)
    return all_dataframes
def mahalanobis(x,mean,cov):
    x_minus_mu = x - mean
    inv_covmat = np.linalg.pinv(cov)
    left_term = np.dot(x_minus_mu, inv_covmat)
    mahal = np.dot(left_term, x_minus_mu.T)
    return mahal

def get_covariance_matrices(group_dataframes):
    covariance_matrices_groups=[]
    for group in group_dataframes:
        # cov = np.cov(group.values.T)
        sp_coef, p = st.spearmanr(group.values.T)
        mad = st.median_abs_deviation(group.values.T, scale = 'normal')
        cov = sp_coef*mad
        covariance_matrices_groups.append(cov)
    return covariance_matrices_groups


def get_mean_vectors(group_dataframes):
    mean_vectors_groups=[]
    for group in group_dataframes:
        mean_vector = np.mean(group, axis=0)
        mean_vectors_groups.append(mean_vector)
    return mean_vectors_groups

def write_final_file(all_predictions):
    df = pd.DataFrame(all_predictions)
    df.columns =['id', 'prediction_gasto_familiar']
    df.to_csv("our_predictions.csv", index=False)

def write_group_file(all_groups):
    df = pd.DataFrame(all_groups)
    df.columns =['id', 'grupo']
    df.to_csv("grouping.csv", index=False)



def clasification_and_prediction(dataframe,group_dataframes,all_groups_parameters,client_ids):
    mean_vector_groups=get_mean_vectors(group_dataframes)
    covariance_matrices_groups=get_covariance_matrices(group_dataframes)
    df_test = dataframe.copy()
    all_predictions=[]
    all_group=[]
    for i in range(len(df_test)):
        x=df_test.iloc[i,:]
        #print("la x a predecir es:")
        #print(x)
        min_dist_mahalanobis=float("inf")
        best_group_number=0
        group_number=0 ##assumes groups start in zero
        for group in group_dataframes:
            dist_mahalanobis=mahalanobis(x,mean_vector_groups[group_number],covariance_matrices_groups[group_number])
            #print(["la distancia de mahalanobis al grupo",group_number,"es",dist_mahalanobis])
            if dist_mahalanobis<min_dist_mahalanobis:
                min_dist_mahalanobis=dist_mahalanobis
                best_group_number=group_number
            group_number=group_number+1
        #print(["la distancia minima es",min_dist_mahalanobis, "y la da el grupo",best_group_number ])
        prediction_for_x = predict_gasto_familiar(x,best_group_number,all_groups_parameters)
        all_predictions.append([client_ids[i],prediction_for_x])
        all_group.append([client_ids[i],best_group_number])
        print(["listo x=",i])
    return all_predictions, all_group

def predict_gasto_familiar(x,best_group_number,all_group_parameters):
    parameters_best_group=all_group_parameters.iloc[best_group_number,:] #saco todos los parametros del mejor grupo
    #print("los parametros del mejor grupo son:")
    #print(parameters_best_group)

    intercept=parameters_best_group.iloc[0]
    #print(["el intercepto es:", intercept])
    coefficients=parameters_best_group[1:]
    #print(["los coeficientes son",coefficients])
    gasto_familiar=intercept+np.dot(x,coefficients)
    #print(["el gasto familiar calculado es ",gasto_familiar])
    return gasto_familiar


if __name__ == "__main__":
    data_types = {
        "periodo":str,
        "id_cli":str,
        "fecha_nacimiento":str,
        "edad":str,
        "genero":str,
        "estado_civil":str,
        "nivel_academico":str,
        "profesion":str,
        "ocupacion":str,
        "tipo_vivienda":str,
        "ult_actual":str,
        "categoria":str,
        "codigo_ciiu":str,
        "ind_mora_vigente":str,
        "cartera_castigada":str,
        "ciudad_residencia":str,
        "departamento_residencia":str,
        "ciudad_laboral":str,
        "departamento_laboral":str,
        "rechazo_credito":str,
        "mora_max":str,
        "cant_moras_30_ult_12_meses":str,
        "cant_moras_60_ult_12_meses":str,
        "cant_moras_90_ult_12_meses":str,
        "cupo_total_tc":str,
        "tenencia_tc":str,
        "cuota_tc_bancolombia":str,
        "tiene_consumo":str,
        "tiene_crediagil":str,
        "nro_tot_cuentas":str,
        "ctas_activas":str,
        "tiene_ctas_activas":str,
        "ctas_embargadas":str,
        "tiene_ctas_embargadas":str,
        "pension_fopep":str,
        "cuota_cred_hipot":str,
        "tiene_cred_hipo_1":str,
        "tiene_cred_hipo_2":str,
        "mediana_nom3":str,
        "mediana_pen3":str,
        "ingreso_nompen":str,
        "cat_ingreso":str,
        "ingreso_final":str,
        "cant_mora_30_tdc_ult_3m_sf":str,
        "cant_mora_30_consum_ult_3m_sf":str,
        "cuota_de_vivienda":str,
        "cuota_de_consumo":str,
        "cuota_rotativos":str,
        "cuota_tarjeta_de_credito":str,
        "cuota_de_sector_solidario":str,
        "cuota_sector_real_comercio":str,
        "cupo_tc_mdo":str,
        "saldo_prom3_tdc_mdo":str,
        "cuota_tc_mdo":str,
        "saldo_no_rot_mdo":str,
        "cuota_libranza_sf":str,
        "cant_oblig_tot_sf":str,
        "cant_cast_ult_12m_sr":str,
        "ind":str,
        "rep_calif_cred":str,
        "pol_centr_ext":str,
        "convenio_lib":str,
        "ingreso_nomina":str,
        "ingreso_segurida_social":str,
        "gasto_familiar":str
    }
    #read the test_set and extract numeric variables
    #data_test= read_file(file_name='data/test_set_cleaned.csv', save_file=False, data_types=data_types)
    
    data_test=pd.read_csv("data/train_set_500_cleaned.csv")
    variables_numericas=[4,5,12,14,15,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,61,63,64]
    variables_numericas=list(np.array(variables_numericas) - 1)


    numeric_data_test=data_test.iloc[:,variables_numericas] ##dataframe with the data that is going to be tested
    header= ['edad', 'genero', 'categoria', 'ind_mora_vigente', 'cartera_castigada', 'cant_moras_30_ult_12_meses', 'cant_moras_60_ult_12_meses', 'cant_moras_90_ult_12_meses','cupo_total_tc','tenencia_tc','cuota_tc_bancolombia',"tiene_consumo","tiene_crediagil",'nro_tot_cuentas','ctas_activas',"tiene_ctas_activas","ctas_embargadas","tiene_ctas_embargadas","pension_fopep","cuota_cred_hipot","tiene_cred_hipo_1","tiene_cred_hipo_2","mediana_nom3","mediana_pen3","ingreso_nompen","ingreso_final","cant_mora_30_tdc_ult_3m_sf","cant_mora_30_consum_ult_3m_sf","cuota_de_vivienda","cuota_de_consumo","cuota_rotativos","cuota_tarjeta_de_credito","cuota_de_sector_solidario","cuota_sector_real_comercio","cupo_tc_mdo","saldo_prom3_tdc_mdo","cuota_tc_mdo","saldo_no_rot_mdo","cuota_libranza_sf","cant_oblig_tot_sf","cant_cast_ult_12m_sr","ind","pol_centr_ext","ingreso_nomina","ingreso_segurida_social"]
    numeric_data_test.columns=header
    numeric_data_test=numeric_data_test.fillna(-1) ###como manejamos los missing values??

    #an array that contain many data_frames which correspond to the dataframes of the groups created
    group_dataframes=data_frames_creator()


    #read the file that contains the parameters of all the groups
    all_group_parameters=pd.read_csv("parameters/parameters.csv")

    #extract the ids of all the individuals that are going to be predicted
    client_ids=data_test.iloc[:,1]

    #call the classification and prediction method
    predictions,grupos=clasification_and_prediction(numeric_data_test,group_dataframes,all_group_parameters,client_ids)

    #with the predictions obtained write a file in the format asked
    write_final_file(predictions)
    write_group_file(grupos)
