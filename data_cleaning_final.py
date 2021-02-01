import csv
import datetime
import time

import pandas as pd
import dask.dataframe as dd
import unidecode
from dask import delayed

global clean_data
def clean_occupation_column():
    """
    This function cleans the occupation column
    in the dataset. Replaces the commas separating
    different occupation with - to avoid reading
    mistakes
    """
    with open('data/train_set_original.csv', 'r') as csv_file:
        csv_read  = csv.reader(csv_file, delimiter=',')
        with open('data/train_set.csv', 'w') as csv_file_write:
            csv_write = csv.writer(csv_file_write, delimiter=',')
            print('Archivos leidos de manera exitosa')

            header     = list(pd.read_table('data/header.txt', delimiter=',').columns)
            csv_write.writerow(header)

            j = 0
            for row in csv_read:
                j = j + 1
                print(f'Recorriendo la linea {j}')
                current_row = row

                try:
                    # Check if ult_act is a numeric value
                    int(current_row[10])
                    csv_write.writerow(current_row)
                    continue
                except:
                    # Since ult_act is not numeric, more than
                    # one occupation has been written
                    print(f'Error en la linea {j}')
                    l = 11 # Starts at 11 because index 10 has been reviewed already
                    while True:
                        try:
                            int(current_row[l])
                            break
                        except:
                            l += 1

                    # Append the cleared content to the file
                    occupation = '-'.join(current_row[8 : l - 1])
                    csv_write.writerow(current_row[ : 8] + [occupation] + current_row[l- 1:])

            csv_file_write.close() # Close and save the write file
        csv_file.close() # Close the read file

def read_file(file_name, save_file, data_types):
    """[summary]

    Args:
        file_name ([type]): [description]
        save_file ([type]): [description]

    Returns:
        [type]: [description]
    """
    # df         = pd.read_csv(file_name, delimiter=',', low_memory=False)
    # header     = list(pd.read_table('data/header.txt', delimiter=',').columns)
    # print(header)
    # df.columns = header

    df         = dd.read_csv(file_name, dtype=data_types)
    # header     = list(pd.read_table('data/header.txt', delimiter=',').columns)
    # df.columns = header

    if save_file:
        df.to_csv('data/holaa.csv', index=False)
        

    return df


@delayed
def clean_period_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        DataFrame: [description]
    """
    df = dataframe.copy()
    new_period=[]
    for x in df.periodo:
        try:
            new_period.append(datetime.datetime.strptime(str(x), '%Y%m').date())
        except:
            x = datetime.datetime(1900,1,1)
            new_period.append(x)

    df['periodo'] = new_period
    print("termino periodo")
    return df

@delayed
def clean_date_of_birth_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        DataFrame: [description]
    """
    df = dataframe.copy()
    new_date_of_birth= []
    for x in df.fecha_nacimiento:
        try:
            new_date_of_birth.append(datetime.datetime.strptime(str(x), '%Y%m%d').date())
        except:
            x = datetime.datetime(1900,1,1)
            new_date_of_birth.append(x)
    df['fecha_nacimiento'] = new_date_of_birth
    print("termino fecha de nacimiento")
    return df


@delayed
def clean_age_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        DataFrame: [description]
    """
    df = dataframe.copy()
    new_age= []
    for x in df.edad:
        try:
            new_age.append(float(x))
        except:
            new_age.append(float(-1))

    df['edad'] = new_age
    print("termino edad")
    return df

@delayed
def clean_gender_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        DataFrame: [description]
    """
    df = dataframe.copy()
    new_gender= []
    for x in df.genero:
        if x=="M":
            new_gender.append(0)
        elif x=="F":
            new_gender.append(1)
        else:
            new_gender.append(-1)
    df['genero'] = new_gender
    print("termino genero")
    return df


@delayed
def clean_civil_status_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """
    df = dataframe.copy()
    df['estado_civil'] = df['estado_civil'].apply(lambda x: x.lower().replace(' ', '_'))
    new_civil_status = []
    for x in df.estado_civil:
        if x=='sin_informacion' or x=="no_informa" or x== "\\n":
           new_civil_status.append('otro')
        else:
            new_civil_status.append(x)

    df['estado_civil'] = new_civil_status

    print("termino estado civil")
    return df

@delayed
def clean_academic_background_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """
    df = dataframe.copy()
    df['nivel_academico'] = df['nivel_academico'].apply(lambda x: x.lower().replace(' ', '_'))
    new_academic_background = []
    for x in df.nivel_academico:
        if x=="no_informa" or x=="sin_informacion" or x=="\\n":
            new_academic_background.append('otro')
        else:
            new_academic_background.append(x)
    df['nivel_academico'] = new_academic_background
    print("termino estudio")

    return df

@delayed
def clean_profesion_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """
    df = dataframe.copy()
    df['profesion'] = df['profesion'].apply(lambda x: x.lower().replace(' ', '_'))
    new_profesion = []
    for x in df.profesion:
        if x=="no_informa" or x=="otros" or x== "\\n" or x=="sin_informacion":
           new_profesion.append('otro')
        else:
            new_profesion.append(x)
            
    df['profesion'] = new_profesion
    print("termino profesion")
    return df

@delayed
def clean_occupation2_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """
    df = dataframe.copy()
    df['ocupacion'] = df['ocupacion'].apply(lambda x: x.lower().strip().replace(' ', '_'))
    new_occupation = []
    for x in df.ocupacion:
        if x=="no_informa" or x=="sin_informacion" or x=="vacío" or x== "\\n":
           new_occupation.append('otro')
        else:
            new_occupation.append(x)
    df['ocupacion'] = new_occupation
    print("termino ocupacion")
    return df

@delayed
def clean_housing_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """
    df = dataframe.copy()
    df['tipo_vivienda'] = df['tipo_vivienda'].apply(lambda x: x.lower().replace(' ', '_'))
    new_housing = []
    for x in df.tipo_vivienda:
        if x=="no_informa" or x=="\\n" or x=="sin_informacion":
            new_housing.append('otro')
        else:
           new_housing.append(x)
    print("termino tipo de vivienda")
    df['tipo_vivienda'] = new_housing


    return df


@delayed
def clean_last_actual_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        DataFrame: [description]
    """
    df = dataframe.copy()
    new_last_actual= []
    for x in df.ult_actual:
        try:
            new_last_actual.append(datetime.datetime.strptime(str(x), '%Y%m%d').date())
        except:
            x = datetime.datetime(1900,1,1)
            new_last_actual.append(x)
    print("termino ult_actual")
    df['ult_actual'] = new_last_actual
    return df


@delayed
def clean_category_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """
    df = dataframe.copy()
    new_category = []
    for x in df.categoria:
        try:
            new_category.append(int(x))
        except:
            new_category.append(-1)
    df['categoria'] = new_category
    print("termino categoria")
    return df

@delayed
def clean_ciiu_code_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """
    df = dataframe.copy()
    new_ciiu_code=[]
    for x in df.codigo_ciiu:
        if x=="NO INFORMA" or x=="\\N" or x=="SIN INFORMACION":
            new_ciiu_code.append('otro')
        else:
           new_ciiu_code.append(x)
    df['codigo_ciiu'] = new_ciiu_code
    print("termino codigo_ciiu")
    return df

@delayed
def clean_ind_mora_vigente_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        DataFrame: [description]
    """
    df = dataframe.copy()
    new_ind_mora_vigente= []
    for x in df.ind_mora_vigente:
        if x=="N":
            new_ind_mora_vigente.append(0)
        elif x=="S":
            new_ind_mora_vigente.append(1)
        else:
            new_ind_mora_vigente.append(-1)

    df['ind_mora_vigente'] = new_ind_mora_vigente
    print("termino ind_mora_vigente")
    return df


@delayed
def clean_cartera_castigada_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        DataFrame: [description]
    """
    df = dataframe.copy()
    new_cartera_castigada= []
    for x in df.cartera_castigada:
        if x=="N":
           new_cartera_castigada.append(0)
        elif x=="S":
            new_cartera_castigada.append(1)
        else:
            new_cartera_castigada.append(-1)

    df['cartera_castigada'] = new_cartera_castigada
    print("termino cartera_castigada")
    return df

@delayed
def clean_residence_city_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """
    df = dataframe.copy()
    new_residence_city = []
    df['ciudad_residencia'] = df['ciudad_residencia'].apply(lambda x: x.lower().strip().replace(' ', '_'))
    for x in df.ciudad_residencia:
        if x== "sin_informacion"or x=="\\n" or x=="no_informa":
           new_residence_city.append('otro')
        else:
            if ('#' in x):
                x = x.replace('#', 'n')
            elif ('ñ' in x):
                x = x.replace('ñ', 'n')
            new_residence_city.append (x)
    df['ciudad_residencia'] = new_residence_city
    print("termino ciudad_residencia")
    return df

@delayed
def clean_residence_department_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    df['departamento_residencia'] = df['departamento_residencia'].apply(lambda x: x.lower().strip().replace(' ', '_'))
    new_residence_department = []
    for x in df.departamento_residencia:
        if x== "sin_informacion" or x=="\\n" or x=="no_informa":
           new_residence_department.append('otro')
        else:
            if ('#' in x):
                x = x.replace('#', 'n')
            elif ('ñ' in x):
                x = x.replace('ñ', 'n')

            new_residence_department.append(x)
    df['departamento_residencia']=new_residence_department
    print("termino departamento_residencia")
    return df

@delayed
def clean_working_city_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    df['ciudad_laboral'] = df['ciudad_laboral'].apply(lambda x: x.lower().strip().replace(' ', '_'))
    new_working_city = []
    for x in df.ciudad_laboral:
        if x== "sin_informacion"or x=="\\n" or x=="no_informa":
           new_working_city.append('otro')
        else:
            if ('#' in x):
                x = x.replace('#', 'n')
            elif ('ñ' in x):
                x = x.replace('ñ', 'n')
            new_working_city.append (x) 
    df['ciudad_laboral'] = new_working_city
    print("termino ciudad_laboral")
    return df


@delayed
def clean_working_department_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """
    df = dataframe.copy()
    df['departamento_laboral'] = df['departamento_laboral'].apply(lambda x: x.lower().strip().replace(' ', '_'))
    new_working_department = []
    for x in df.departamento_laboral:
        if x== "sin_informacion" or x=="\\n" or x=="no_informa" :
           new_working_department.append('otro')
        else:
            if ('#' in x):
                x = x.replace('#', 'n')
            elif ('ñ' in x):
                x = x.replace('ñ', 'n')

            new_working_department.append(x)
    df['departamento_laboral']=new_working_department
    print("termino departamento_laboral")
    return df

@delayed
def clean_reject_credit_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        DataFrame: [description]
    """
    df = dataframe.copy()
    df['rechazo_credito'] = df['rechazo_credito'].apply(lambda x: x.lower().strip().replace(' ', '_'))
    new_reject_credit= []
    for x in df.rechazo_credito:
        if x== "\\n":
           new_reject_credit.append("otro")
        else:
           new_reject_credit.append(x)
    df['rechazo_credito'] = new_reject_credit
    print("termino rechazo_credito")
    return df

@delayed
def clean_mora_max_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        DataFrame: [description]
    """
    df = dataframe.copy()
    new_mora_max= []
    for x in df.mora_max:
        if x== "\\N":
           new_mora_max.append("otro")
        else:
           new_mora_max.append(x)
    df['mora_max'] = new_mora_max
    print("termino mora_max")
    return df

@delayed
def clean_q_mora_30(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_q_mora_30 = []
    for x in df.cant_moras_30_ult_12_meses:
        if x== "SIN INFORMACIÓN"or x=="\\N" :
           new_q_mora_30.append(int('-1'))
        else:
            new_q_mora_30.append (int(x.strip())) # x.strip removes all blank spaces
    df['cant_moras_30_ult_12_meses'] = new_q_mora_30
    print("Mora 30")
    return df

@delayed
def clean_q_mora_60(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_q_mora_60 = []
    for x in df.cant_moras_60_ult_12_meses:
        if x== "SIN INFORMACIÓN"or x=="\\N" :
           new_q_mora_60.append(int('-1'))
        else:
            new_q_mora_60.append (int(x.strip())) # x.strip removes all blank spaces
    df['cant_moras_60_ult_12_meses'] = new_q_mora_60
    print("Mora 60")
    return df

@delayed
def clean_q_mora_90(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_q_mora_90 = []
    for x in df.cant_moras_90_ult_12_meses:
        if x== "SIN INFORMACIÓN"or x=="\\N" :
           new_q_mora_90.append(int('-1'))
        else:
            new_q_mora_90.append (int(x.strip())) # x.strip removes all blank spaces
    df['cant_moras_90_ult_12_meses'] = new_q_mora_90
    print("Mora 90")
    return df

@delayed
def clean_q_cc(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_q_cc = []
    for x in df.cupo_total_tc:
        if x== "SIN INFORMACIÓN"or x=="\\N" :
           new_q_cc.append(float('-1'))
        else:
            new_q_cc.append (float(x.strip())) # x.strip removes all blank spaces
    df['cupo_total_tc'] = new_q_cc
    print("cupo_total_tc")
    return df

@delayed
def clean_has_cc(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_has_cc = []
    for x in df.tenencia_tc:
        if x=="\\N" :
           new_has_cc.append(int('-1'))
        elif x=="SI" :
            new_has_cc.append(int('1'))
        elif x=="NO":
            new_has_cc.append(int('0'))
        else:
            new_has_cc.append (x.strip()) # x.strip removes all blank spaces
    df['tenencia_tc'] = new_has_cc
    print("tenencia_tc")
    return df
        
@delayed
def clean_fee_cc(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_fee_cc = []
    for x in df.cuota_tc_bancolombia:
        if x== "SIN INFORMACIÓN"or x=="\\N" :
           new_fee_cc.append(float('-1'))
        else:
            new_fee_cc.append (float(x.strip())) # x.strip removes all blank spaces
    df['cuota_tc_bancolombia'] = new_fee_cc
    print("cuota_tc_bancolombia")
    return df

@delayed
def clean_has_consump(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_has_consump = []
    for x in df.tiene_consumo :
        if x=="\\N" :
           new_has_consump.append(int('0'))
        else :
            new_has_consump.append(int('1'))
    df['tiene_consumo'] = new_has_consump
    print("tiene_consumo")
    return df

@delayed
def clean_has_crediagil(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_has_crediagil = []
    for x in df.tiene_crediagil :
        if x=="\\N" :
           new_has_crediagil.append(int('0'))        
        else:
            new_has_crediagil.append(int('1'))
    df['tiene_crediagil'] = new_has_crediagil
    print("tiene_crediagil")
    return df

@delayed
def clean_num_acc(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_num_acc = []
    for x in df.nro_tot_cuentas :
        if x=="\\N" :
           new_num_acc.append(int('-1'))        
        else:            
            new_num_acc.append (int(x.strip())) # x.strip removes all blank spaces
    df['nro_tot_cuentas'] = new_num_acc
    print("nro_tot_cuentas")
    return df

@delayed
def clean_act_acc(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_act_acc = []
    for x in df.ctas_activas :
        if x=="\\N" :
           new_act_acc.append(int('-1'))
        else:            
            new_act_acc.append (int(x.strip())) # x.strip removes all blank spaces
    df['ctas_activas'] = new_act_acc
    print("ctas_activas")
    return df

@delayed
def clean_has_act_acc(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_has_act_acc = []
    for x in df.tiene_ctas_activas :
        if x=="\\N" :
           new_has_act_acc.append(int('0'))
        elif x=='X' :
            new_has_act_acc.append(int('1'))
        else:            
            new_has_act_acc.append (x.strip()) # x.strip removes all blank spaces
    df['tiene_ctas_activas'] = new_has_act_acc
    print("tiene_ctas_activas")
    return df

@delayed
def clean_garn_acc(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_garn_acc = []
    for x in df.ctas_embargadas:
        if x=="\\N" :
           new_garn_acc.append(int('-1'))
        else:
            new_garn_acc.append (int(x.strip())) # x.strip removes all blank spaces
    df['ctas_embargadas'] = new_garn_acc
    print("ctas_embargadas")
    return df

@delayed
def clean_has_garn_acc(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_has_garn_acc = []
    for x in df.tiene_ctas_embargadas :
        if x=="\\N" :
           new_has_garn_acc.append(int('0'))
        elif x=='X' :
            new_has_garn_acc.append(int('1'))
        else:            
            new_has_garn_acc.append (x.strip()) # x.strip removes all blank spaces
    df['tiene_ctas_embargadas'] = new_has_garn_acc
    print("tiene_ctas_embargadas")
    return df

@delayed
def clean_pens_fund(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_pens_fund = []
    for x in df.pension_fopep :
        if x=="\\N" :
           new_pens_fund.append(int('0'))        
        elif x=='X' :
            new_pens_fund.append(int('1'))
        else:            
            new_pens_fund.append (x.strip()) # x.strip removes all blank spaces
    df['pension_fopep'] = new_pens_fund
    print("pension_fopep")
    return df

@delayed
def clean_fee_hip_cred(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_hip_cred = []
    for x in df.cuota_cred_hipot:
        if x== "SIN INFORMACIÓN"or x=="\\N" :
           new_hip_cred.append(float('-1'))
        else:
            new_hip_cred.append (float(x.strip())) # x.strip removes all blank spaces
    df['cuota_cred_hipot'] = new_hip_cred
    print("cuota_cred_hipot")
    return df

@delayed
def clean_has_hip_cred1(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_tiene_cred_hipo_1 = []
    for x in df.tiene_cred_hipo_1 :
        if x=="X" :
           new_tiene_cred_hipo_1.append(int('1'))        
        else:
            new_tiene_cred_hipo_1.append(int('0'))
    df['tiene_cred_hipo_1'] = new_tiene_cred_hipo_1
    print("tiene_cred_hipo_1")
    return df

@delayed
def clean_has_hip_cred2(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_tiene_cred_hipo_2 = []
    for x in df.tiene_cred_hipo_2 :
        if x=="X" :
           new_tiene_cred_hipo_2.append(int('1'))
        else:
            new_tiene_cred_hipo_2.append(int('0'))
    df['tiene_cred_hipo_2'] = new_tiene_cred_hipo_2
    print("tiene_cred_hipo_2")
    return df

@delayed
def clean_med_nom3(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_med_nom3 = []
    for x in df.mediana_nom3:
        if x== "SIN INFORMACIÓN"or x=="\\N" :
           new_med_nom3.append(float('-1'))
        else:
            new_med_nom3.append (float(x.strip())) # x.strip removes all blank spaces
    df['mediana_nom3'] = new_med_nom3
    print("mediana_nom3")
    return df

@delayed
def clean_med_pen3(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_med_pen3 = []
    for x in df.mediana_pen3:
        if x== "SIN INFORMACIÓN"or x=="\\N" :
           new_med_pen3.append(float('-1'))
        else:
            new_med_pen3.append (float(x.strip())) # x.strip removes all blank spaces
    df['mediana_pen3'] = new_med_pen3
    print("mediana_pen3")
    return df

@delayed
def clean_nompen(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_nompen = []
    for x in df.ingreso_nompen:
        if x== "SIN INFORMACIÓN"or x=="\\N" :
           new_nompen.append(float('-1'))
        else:
            new_nompen.append (float(x.strip())) # x.strip removes all blank spaces
    df['ingreso_nompen'] = new_nompen
    print("ingreso_nompen")
    return df

@delayed
def clean_in_q(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_in_q = []
    for x in df.cat_ingreso:
        if x=="\\N" :
           new_in_q.append('OTRO')
        else:
            new_in_q.append (x.strip()) # x.strip removes all blank spaces
    df['cat_ingreso'] = new_in_q
    df['cat_ingreso'] = df['cat_ingreso'].apply(lambda x: x.lower().replace(' ', '_'))
    print("cat_ingreso")
    return df

@delayed
def clean_final_in(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()
    new_final_in = []
    for x in df.ingreso_final:
        if x== "SIN INFORMACIÓN"or x=="\\N" :
           new_final_in.append(float('-1'))
        else:
            new_final_in.append (float(x)) # x.strip removes all blank spaces
    df['ingreso_final'] = new_final_in
    print("ingreso_final")
    return df


@delayed
def clean_cant_mora_30_tdc_ult_3m_sf_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    new_credit_card_debt = []
    for x in df.cant_mora_30_tdc_ult_3m_sf:
        if x=='\\N' :
           new_credit_card_debt.append(-1)
        else:
            new_credit_card_debt.append(int(x)) 

    df['cant_mora_30_tdc_ult_3m_sf'] = new_credit_card_debt

    print(f'Variable 1 terminó')

    return df

@delayed
def clean_cant_mora_30_consum_ult_3m_sf_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    new_consumption_debt = []
    for x in df.cant_mora_30_consum_ult_3m_sf:
        if x=='\\N' :
           new_consumption_debt.append(-1)
        else:
            new_consumption_debt.append(int(x)) 

    df['cant_mora_30_consum_ult_3m_sf'] = new_consumption_debt

    print(f'Variable 2 terminó')

    return df

@delayed
def clean_cuota_de_vivienda_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['cuota_de_vivienda'] = df['cuota_de_vivienda'].apply(lambda x: float(x))

    print(f'Variable 3 terminó')

    return df

@delayed
def clean_cuota_de_consumo_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['cuota_de_consumo'] = df['cuota_de_consumo'].apply(lambda x: float(x))

    print(f'Variable 4 terminó')

    return df

@delayed
def clean_cuota_rotativos_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['cuota_rotativos'] = df['cuota_rotativos'].apply(lambda x: float(x))

    print(f'Variable 5 terminó')

    return df

@delayed
def clean_cuota_tarjeta_de_credito_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['cuota_tarjeta_de_credito'] = df['cuota_tarjeta_de_credito'].apply(lambda x: float(x))

    print(f'Variable 6 terminó')

    return df


@delayed
def clean_cuota_de_sector_solidario_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['cuota_de_sector_solidario'] = df['cuota_de_sector_solidario'].apply(lambda x: float(x))

    print(f'Variable 7 terminó')

    return df

@delayed
def clean_cuota_sector_real_comercio_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['cuota_sector_real_comercio'] = df['cuota_sector_real_comercio'].apply(lambda x: int(x))

    print(f'Variable 8 terminó')

    return df

@delayed
def clean_cupo_tc_mdo_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['cupo_tc_mdo'] = df['cupo_tc_mdo'].apply(lambda x: int(x))

    print(f'Variable 9 terminó')

    return df

@delayed
def clean_saldo_prom3_tdc_mdo_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['saldo_prom3_tdc_mdo'] = df['saldo_prom3_tdc_mdo'].apply(lambda x: float(x))

    print(f'Variable 10 terminó')

    return df

@delayed
def clean_cuota_tc_mdo_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['cuota_tc_mdo'] = df['cuota_tc_mdo'].apply(lambda x: int(x))

    print(f'Variable 11 terminó')

    return df

@delayed
def clean_saldo_no_rot_mdo_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['saldo_no_rot_mdo'] = df['saldo_no_rot_mdo'].apply(lambda x: int(x))

    print(f'Variable 12 terminó')

    return df

@delayed
def clean_cuota_libranza_sf_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['cuota_libranza_sf'] = df['cuota_libranza_sf'].apply(lambda x: int(x))

    print(f'Variable 13 terminó')

    return df

@delayed
def clean_cant_oblig_tot_sf_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    number_obligations = []
    for x in df.cant_oblig_tot_sf:
        if x=='\\N' :
            number_obligations.append(-1)
        else:
            number_obligations.append(int(x)) 

    df['cant_oblig_tot_sf'] = number_obligations

    print(f'Variable 14 terminó')

    return df

@delayed
def clean_cant_cast_ult_12m_sr_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    penalized_portfolios = []
    for x in df.cant_cast_ult_12m_sr:
        if x=='\\N' :
            penalized_portfolios.append(-1)
        else:
            penalized_portfolios.append(int(x)) 

    df['cant_cast_ult_12m_sr'] = penalized_portfolios

    print(f'Variable 15 terminó')

    return df

@delayed
def clean_ind_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    new_ind = []
    for x in df.ind:
        if x=='\\N' :
            new_ind.append(-1)
        else:
            new_ind.append(float(x)) 

    df['ind'] = new_ind

    print(f'Variable 16 terminó')

    return df

@delayed
def clean_rep_calif_cred_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['rep_calif_cred'] = df['rep_calif_cred'].apply(lambda x: x.lower().strip().replace(' ', '_'))

    new_rep_calif_cred = []
    for x in df.rep_calif_cred:
        if x=='sin_info' :
            new_rep_calif_cred.append('otro')
        else:
            new_rep_calif_cred.append(x) 

    df['rep_calif_cred'] = new_rep_calif_cred

    print(f'Variable 17 terminó')

    return df

@delayed
def clean_pol_centr_ext_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    new_pol_centr_ext = []
    for x in df.pol_centr_ext:
        if x=='\\N' :
            new_pol_centr_ext.append(-1)
        else:
            new_pol_centr_ext.append(int(x)) 

    df['pol_centr_ext'] = new_pol_centr_ext

    print(f'Variable 18 terminó')

    return df

@delayed
def clean_convenio_lib_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['convenio_lib'] = df['convenio_lib'].apply(lambda x: x.lower().strip().replace(' ', '_'))

    new_convenio_lib = []
    for x in df.convenio_lib:
        if x=='\\n' or x=='revisar_convenios':
            new_convenio_lib.append(-1)
        else:
            new_convenio_lib.append(int(x)) 

    df['convenio_lib'] = new_convenio_lib

    print(f'Variable 19 terminó')

    return df

@delayed
def clean_ingreso_nomina_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    new_ingreso_nomina = []
    for x in df.ingreso_nomina:
        if x=='\\N':
            new_ingreso_nomina.append(-1)
        else:
            new_ingreso_nomina.append(float(x)) 

    df['ingreso_nomina'] = new_ingreso_nomina

    print(f'Variable 20 terminó')

    return df

@delayed
def clean_ingreso_segurida_social_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    new_ingreso_segurida_social = []
    for x in df.ingreso_segurida_social:
        if x=='\\N':
            new_ingreso_segurida_social.append(-1)
        else:
            new_ingreso_segurida_social.append(int(x)) 

    df['ingreso_segurida_social'] = new_ingreso_segurida_social

    print(f'Variable 21 terminó')

    return df

@delayed
def clean_gasto_familiar_column(dataframe):
    """[summary]

    Args:
        dataframe ([type]): [description]

    Returns:
        [type]: [description]
    """    
    df = dataframe.copy()

    df['gasto_familiar'] = df['gasto_familiar'].apply(lambda x: float(x))

    print(f'Variable 22 terminó')

    return df

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

    #clean_occupation_column()
    data = read_file(file_name='data/test_set.csv', save_file=False, data_types=data_types)
    data = delayed(clean_period_column)(dataframe=data)
    data = delayed(clean_date_of_birth_column)(dataframe=data)
    data = delayed(clean_age_column)(dataframe=data)
    data = delayed(clean_gender_column)(dataframe=data)
    data = delayed(clean_civil_status_column)(dataframe=data)
    data = delayed(clean_academic_background_column)(dataframe=data)
    data = delayed(clean_profesion_column)(dataframe=data)
    data = delayed(clean_occupation2_column)(dataframe=data)
    data = delayed(clean_housing_column)(dataframe=data)
    data= delayed(clean_last_actual_column)(dataframe=data)
    data= delayed(clean_category_column)(dataframe=data)
    data= delayed(clean_ciiu_code_column)(dataframe=data)
    data= delayed(clean_ind_mora_vigente_column)(dataframe=data)
    data= delayed(clean_cartera_castigada_column)(dataframe=data)
    data = delayed(clean_residence_city_column)(dataframe=data)
    data = delayed(clean_residence_department_column)(dataframe=data)
    data = delayed(clean_working_city_column)(dataframe=data)
    data = delayed(clean_working_department_column)(dataframe=data)
    data = delayed(clean_reject_credit_column)(dataframe=data)
    data = delayed(clean_mora_max_column)(dataframe=data)
    data = delayed(clean_q_mora_30)(dataframe=data)
    data = delayed(clean_q_mora_60)(dataframe=data)
    data = delayed(clean_q_mora_90)(dataframe=data)
    data = delayed(clean_q_cc)(dataframe=data)
    data = delayed(clean_has_cc)(dataframe=data)
    data = delayed(clean_fee_cc)(dataframe=data)
    data = delayed(clean_has_consump)(dataframe=data)
    data = delayed(clean_has_crediagil)(dataframe=data)
    data = delayed(clean_num_acc)(dataframe=data)
    data = delayed(clean_act_acc)(dataframe=data)
    data = delayed(clean_has_act_acc)(dataframe=data)
    data = delayed(clean_garn_acc)(dataframe=data)
    data = delayed(clean_has_garn_acc)(dataframe=data)
    data = delayed(clean_pens_fund)(dataframe=data)
    data = delayed(clean_fee_hip_cred)(dataframe=data)
    data = delayed(clean_has_hip_cred1)(dataframe=data)
    data = delayed(clean_has_hip_cred2)(dataframe=data)
    data = delayed(clean_med_nom3)(dataframe=data)
    data = delayed(clean_med_pen3)(dataframe=data)
    data = delayed(clean_nompen)(dataframe=data)
    data = delayed(clean_in_q)(dataframe=data)
    data = delayed(clean_final_in)(dataframe=data)
    data = delayed(clean_cant_mora_30_tdc_ult_3m_sf_column)(dataframe=data)
    data = delayed(clean_cant_mora_30_consum_ult_3m_sf_column)(dataframe=data)
    data = delayed(clean_cuota_de_vivienda_column)(dataframe=data)
    data = delayed(clean_cuota_de_consumo_column)(dataframe=data)
    data = delayed(clean_cuota_rotativos_column)(dataframe=data)
    data = delayed(clean_cuota_tarjeta_de_credito_column)(dataframe=data)
    data = delayed(clean_cuota_de_sector_solidario_column)(dataframe=data)
    data = delayed(clean_cuota_sector_real_comercio_column)(dataframe=data)
    data = delayed(clean_cupo_tc_mdo_column)(dataframe=data)
    data = delayed(clean_saldo_prom3_tdc_mdo_column)(dataframe=data)
    data = delayed(clean_cuota_tc_mdo_column)(dataframe=data)
    data = delayed(clean_saldo_no_rot_mdo_column)(dataframe=data)
    data = delayed(clean_cuota_libranza_sf_column)(dataframe=data)
    data = delayed(clean_cant_oblig_tot_sf_column)(dataframe=data)
    data = delayed(clean_cant_cast_ult_12m_sr_column)(dataframe=data)
    data = delayed(clean_ind_column)(dataframe=data)
    data = delayed(clean_rep_calif_cred_column)(dataframe=data)
    data = delayed(clean_pol_centr_ext_column)(dataframe=data)
    data = delayed(clean_convenio_lib_column)(dataframe=data)
    data = delayed(clean_ingreso_nomina_column)(dataframe=data)
    data = delayed(clean_ingreso_segurida_social_column)(dataframe=data)
    data = delayed(clean_gasto_familiar_column)(dataframe=data)
    data.to_csv('test_set_cleaned.csv', index=False).compute()
