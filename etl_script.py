import pandas as pd     
import os
import sys
import warnings
from datetime import date, datetime
import gc

sys.path.insert(1, 'dependencies')
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
pd.options.mode.chained_assignment = None

def setting_config_paths():

    dist_config_path = '../distributors_config.xlsx'
    data_dictionary_path = '../data_dictionary.xlsx'
    de_para_products_path = '../de_para_products_ecom.xlsx'
    neogrid_template_path = '../TEMPLATE Neogrid.xlsx'

    return dist_config_path, data_dictionary_path, de_para_products_path, neogrid_template_path


def loading_config_information(dist_config_path, data_dictionary_path,
    de_para_products_path, neogrid_template_path):

    df_dist_config = pd.read_excel(open(dist_config_path, 'rb'), dtype=str, header=0).fillna('')
    df_data_dict = pd.read_excel(data_dictionary_path, dtype=str, header=0).fillna('')
    df_de_para_products = pd.read_excel(de_para_products_path, dtype=str, header=0).fillna('')
    df_neogrid_template = pd.read_excel(neogrid_template_path, dtype=str, header=0).fillna('')
    
    return df_dist_config, df_data_dict, df_de_para_products, df_neogrid_template


def sanitizing_config_file(df_dist_config):

    columns = df_dist_config.columns

    for single_column in columns:
        df_dist_config[single_column] = df_dist_config[single_column].str.strip()
    
    df_dist_config['to_be_processed'] = df_dist_config['to_be_processed'].str.lower()

    #Dropping distributors flagged not to be processed
    to_be_dropped = df_dist_config[df_dist_config['to_be_processed'] != 'y'].index
    df_dist_config.drop(to_be_dropped, inplace=True)

    return df_dist_config


def sanitizing_data_dictionary(df_data_dict):

    for column in df_data_dict.columns:
        df_data_dict[column] = df_data_dict[column].str.strip()
    
    df_data_dict.set_index(['Neogrid_template'], inplace=True)

    return df_data_dict


def sanitizing_de_para_products(df_de_para_products):

    for column in df_de_para_products.columns:
        df_de_para_products[column] = df_de_para_products[column].str.strip()
    
    #Making sure that every element of index is gonna be compared with lower case characteres
    df_de_para_products['key_distributor'] = df_de_para_products['VAREJO'].str.upper()
    
    return df_de_para_products


def filtering_config_info(df_dist_config):

    df_dist_config.set_index(['distributor'], inplace=True)
    df_dist_config = df_dist_config[~df_dist_config.index.duplicated(keep='first')]

    dists_individual_info_list = {}

    columns_df_dist_config = df_dist_config.columns

    #Creating Dictionary per dist of static fields and appending to list
    for single_distributor in df_dist_config.index:
        dist_static_dict = {}

        for single_column in columns_df_dist_config:
            dist_static_dict[single_column] = df_dist_config.loc[single_distributor, single_column]
        
        dists_individual_info_list[single_distributor] = dist_static_dict

    return dists_individual_info_list


def loading_input_file(distributor, dists_individual_info_list):

    # If there's specific script file to the distributor, and if the file contains Date Column
    # This Date column has to be returned as a Datetime() object

    folder_name = dists_individual_info_list[distributor]['folder_name']
    header = int(dists_individual_info_list[distributor]['header'])
    has_specific_script = dists_individual_info_list[distributor]['script_file']
    extra_arg = dists_individual_info_list[distributor]['extra_arg']
    input_date_format = dists_individual_info_list[distributor]['date_format']

    input_directory = '../' + folder_name + '/Input'

    if not os.path.isdir(input_directory):
        print('Folder does not exist - {}'.format(input_directory))
        sys.exit(1)

    #Checking if there is exactly one file in the referred directory
    if len(os.listdir(input_directory)) > 1:
        print('Error: More than one input file to be processed - {}'.format(folder_name))
        sys.exit(1)
    elif (len(os.listdir(input_directory)) == 0):
        print('No input files in {}'.format(folder_name))
        sys.exit(1)
    else:
        input_file_name = input_directory + '/' + os.listdir(input_directory)[0]

        if has_specific_script:
            specific_input_script = __import__(has_specific_script)
            df_input = specific_input_script.loading_df_input(input_file_name, header, input_date_format, extra_arg)
            return df_input, input_file_name
        df_input = pd.read_excel(input_file_name, dtype=str, header=header).fillna('')
    return df_input, input_file_name


def assigning_columns(df_dist_config, df_data_dict, distributor, df_neogrid_template, df_input):
    
    #Creating list of keys of dict
    #(Remembering that data_dict index has been set in previous function) - That's because [:5] and not [:6]
    columns_to_be_checked = df_dist_config.columns[6:]

    #Creating list of values of dict
    list_of_values_dist_config = df_dist_config.loc[distributor].to_list()[6:]

    #Nesting structure. Creating dict with keys and values
    dict_columns_vs_values_single_dist = dict(zip(columns_to_be_checked, list_of_values_dist_config))

    #This block will remove the empty values. It means the columns that doesn't have static values
    for key, value in dict_columns_vs_values_single_dist.copy().items():
        if not value:
            del dict_columns_vs_values_single_dist[key]
    
    list_of_non_empty_values_in_static_dist = list(dict_columns_vs_values_single_dist.keys())

    for column_field_name in df_data_dict.index:
        try:
            if column_field_name not in list_of_non_empty_values_in_static_dist:
                df_neogrid_template[column_field_name] = df_input[df_data_dict.loc[column_field_name, distributor]]
        except KeyError as error:
            print('{} : {} : {} - unmapped column'.format(distributor, column_field_name, error))
    

    for column_field_name, static_value in dict_columns_vs_values_single_dist.items():
        df_neogrid_template[column_field_name] = static_value

    return df_neogrid_template


def declaring_de_para_dates(month):

    de_para_quarter = {
        '07':['1','Jul'],
        '08':['1','Ago'],
        '09':['1','Set'],
        '10':['2','Out'],
        '11':['2','Nov'],
        '12':['2','Dez'],
        '01':['3','Jan'],
        '02':['3','Fev'],
        '03':['3','Mar'],
        '04':['4','Abr'],
        '05':['4','Mai'],
        '06':['4','Jun']
    }

    return de_para_quarter[month]


def processing_dates(dists_individual_info_list, distributor, df_neogrid_template, df_input):

    input_date_format = dists_individual_info_list[distributor]['date_format']
    df_neogrid_template['Dia'] = pd.to_datetime(df_neogrid_template['Dia'], format=input_date_format, errors='coerce')
    
    day = df_neogrid_template.loc[0, 'Dia']

    year = str(day.year)
    month = str(day.month).zfill(2)
    trim = year + ' Trimestre ' + declaring_de_para_dates(month)[0]
    month = year + ' ' + declaring_de_para_dates(month)[1]

    year_month_to_create_time_stamp = year + month

    dates = {
        'year' : year,
        'month' : month,
        'trim' : trim,
        'time_stamp': year_month_to_create_time_stamp
    }

    return dates, df_neogrid_template


def filling_dates_into_neogrid_template(df_neogrid_template, dates):

    df_neogrid_template['Ano'] = dates['year']
    df_neogrid_template['Trimestre'] = dates['trim']
    df_neogrid_template['Mês'] = dates['month']
    df_neogrid_template['Semana'] = df_neogrid_template['Dia'].dt.week

    #Extracting just the date from the <Dia> column
    df_neogrid_template['Dia'] = df_neogrid_template['Dia'].dt.date

    return df_neogrid_template


def sanitizing_neogrid_template(df_neogrid_template):

    df_neogrid_template['Quantidade Venda (unidade)'] = df_neogrid_template['Quantidade Venda (unidade)'].fillna(0)
    df_neogrid_template['Valor de Venda'] = pd.to_numeric(df_neogrid_template['Valor de Venda'], errors='coerce').fillna(0)
    df_neogrid_template['Valor de Venda'] = df_neogrid_template['Valor de Venda'].round(2)

    #Dropping rows where EANs are not filled
    df_neogrid_template.drop(df_neogrid_template[df_neogrid_template['EAN Produto Fabricante']==''].index, inplace=True)

    return df_neogrid_template


def slicing_de_para_products(df_de_para_products):

    df_non_diageo_products = df_de_para_products[df_de_para_products['SKU'] == '-1']
    df_acessorios = df_de_para_products[df_de_para_products['SKU'] == '-2']

    df_diageo_products = df_de_para_products.drop(df_non_diageo_products.index)
    df_diageo_products = df_diageo_products.drop(df_acessorios.index)

    df_diageo_products.set_index(['key_distributor', 'EAN'], inplace=True)

    return df_diageo_products, df_non_diageo_products, df_acessorios


def ean_validation(distributor, df_neogrid_template, df_diageo_products, df_non_diageo_products, df_acessorios):

    df_neogrid_template['key_VAREJO'] = df_neogrid_template['Nome do Varejo'].str.upper()
    df_neogrid_template['key_EAN'] = df_neogrid_template['EAN Produto Fabricante']

    #filtering_elements_non_diageo from dataframe(column) of all EAN's and removing them
    to_be_dropped_non_diageo = df_neogrid_template[df_neogrid_template['EAN Produto Fabricante'].isin(df_non_diageo_products['EAN'])]
    df_neogrid_template.drop(to_be_dropped_non_diageo.index, inplace=True)

    #Overwriting df_acessorios with df in Neogrid Format
    df_acessorios_neogrid_template = df_neogrid_template[df_neogrid_template['EAN Produto Fabricante'].isin(df_acessorios['EAN'])]
    df_neogrid_template.drop(df_acessorios_neogrid_template.index, inplace=True)

    df_neogrid_template.set_index(['key_VAREJO', 'key_EAN'], inplace=True)

    new_products_indexes = ~df_neogrid_template.index.isin(df_diageo_products.index)

    df_new_products = df_neogrid_template[new_products_indexes][['Nome do Varejo', 'EAN Produto Fabricante', 'Descrição Produto Fabricante']]
    df_new_products.reset_index(drop=True, inplace=True)
    df_neogrid_template.reset_index(drop=True, inplace=True)

    return df_neogrid_template, df_new_products, df_acessorios_neogrid_template


def assigning_columns_acessorios(df_acessorios, df_acessorios_neogrid_template):

    df_acessorios_neogrid_template = df_acessorios_neogrid_template[['key_VAREJO', 'key_EAN',
    'Nome do Varejo', 'EAN Produto Fabricante', 'Dia', 'Descrição Produto Fabricante',
    'Quantidade Venda (unidade)', 'Valor de Venda']]

    df_acessorios_neogrid_template.rename(columns={ 'Quantidade Venda (unidade)':'Depletion Volume Bottles',
        'Valor de Venda':'Depletion RSV', 'Dia': 'Date Formatted',
        'Descrição Produto Fabricante': 'L5 - Individual Variant' }, inplace=True)

    df_acessorios_neogrid_template.set_index(['key_VAREJO', 'key_EAN'], inplace=True)

    df_acessorios.set_index(['VAREJO', 'EAN'], inplace=True)
    df_acessorios = df_acessorios[~df_acessorios.index.duplicated(keep='first')]

    for single_index in df_acessorios_neogrid_template.index:
        df_acessorios_neogrid_template.loc[single_index, 'L3 - Brand'] = df_acessorios.loc[single_index, 'BRAND']
        df_acessorios_neogrid_template.loc[single_index, 'L6 - Volume'] = df_acessorios.loc[single_index, 'VOLUME']

    df_acessorios_neogrid_template['L2 - Product Group'] = 'Acessórios'
    df_acessorios_neogrid_template['Depletion Volume EU'] = df_acessorios_neogrid_template['Depletion Volume Bottles']

    df_acessorios_neogrid_template = df_acessorios_neogrid_template[['Date Formatted','L2 - Product Group',
        'L3 - Brand', 'L5 - Individual Variant', 'L6 - Volume',
        'Depletion Volume EU', 'Depletion Volume Bottles', 'Depletion RSV']]

    df_acessorios_neogrid_template.reset_index(drop=True, inplace=True)

    return df_acessorios_neogrid_template


def writing_new_products_file(distributor, input_file_name, df_new_products, timestamp_year_and_month):

    final_time_stamp = timestamp_year_and_month + datetime.today().strftime("%H%M%S")
    new_products_path = '/'.join(input_file_name.split('/')[:-2]) + '/New_products'
    new_products_file_name = new_products_path + '/NEW_Prod_' + distributor + '_' + final_time_stamp + '.xlsx'

    if not os.path.isdir(new_products_path):
        os.mkdir(new_products_path)
    
    df_new_products.to_excel(new_products_file_name, index=False)

    return True


def writing_acessorios_file(distributor, input_file_name, df_acessorios_neogrid_template, timestamp_year_and_month):

    final_time_stamp = timestamp_year_and_month + datetime.today().strftime("%H%M%S")
    acessorios_path = '/'.join(input_file_name.split('/')[:-2]) + '/Acessorios'
    acessorios_file_name = acessorios_path + '/Acessorios_' + distributor + '_' + final_time_stamp + '.xlsx'

    if not os.path.isdir(acessorios_path):
        os.mkdir(acessorios_path)
    
    df_acessorios_neogrid_template.to_excel(acessorios_file_name, index=False)

    return True


def moving_input_file_to_archive(input_file_name, timestamp_year_and_month):
    
    final_time_stamp = timestamp_year_and_month + datetime.today().strftime("%H%M%S")
    archive_path = '/'.join(input_file_name.split('/')[:-2]) + '/Archive'
    if not os.path.isdir(archive_path):
        os.mkdir(archive_path)

    file_moved_path = archive_path + '/' + input_file_name.split('/')[-1] + final_time_stamp + 'archived'
    os.rename(input_file_name, file_moved_path)

    return True


def writing_neogrid_template_file(df_neogrid_template, distributor, input_file_name, timestamp_year_and_month):

    final_time_stamp = timestamp_year_and_month + datetime.today().strftime("%H%M%S")
    output_path = '/'.join(input_file_name.split('/')[:-2]) + '/Output'
    output_file_name = output_path + '/DBD_DIAGEO_' + distributor + '_' + final_time_stamp

    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    df_neogrid_template.to_excel(output_file_name + '.xlsx', index=False)
    df_neogrid_template.to_csv(output_file_name + '.txt', index=False, sep=';', encoding='mbcs')
    
    return True

def main():

    try:
        print('setting_config_paths')
        dist_config_path, data_dictionary_path, de_para_products_path, neogrid_template_path = setting_config_paths()
    except Exception as error:
        print(error)
        sys.exit(1)

    try:
        print('loading_config_information')
        df_dist_config, df_data_dict, df_de_para_products, df_neogrid_template = loading_config_information(dist_config_path, 
            data_dictionary_path, de_para_products_path, neogrid_template_path)
    except Exception as error:
        print(error)
        sys.exit(1)

    try:
        print('sanitizing_config_file')
        df_dist_config = sanitizing_config_file(df_dist_config)
    except Exception as error:
        print(error)
        sys.exit(1)
    
    try:
        print('sanitizing_data_dictionary')
        df_data_dict = sanitizing_data_dictionary(df_data_dict)
    except Exception as error:
        print(error)
        sys.exit(1)
    
    try:
        print('sanitizing_de_para_products')
        df_de_para_products = sanitizing_de_para_products(df_de_para_products)
    except Exception as error:
        print(error)
        sys.exit(1)

    try:
        print('filtering_config_info')
        dists_individual_info_list = filtering_config_info(df_dist_config)
    except Exception as error:
        print(error)
        sys.exit(1)

    if dists_individual_info_list:
        for distributor in dists_individual_info_list:
            
            try:
                print('loading_input_file {}'.format(distributor))
                df_input, input_file_name = loading_input_file(distributor, dists_individual_info_list)
            except Exception as error:
                print(error)
                sys.exit(1)

            try:
                print('assigning_columns')
                df_neogrid_template = assigning_columns(df_dist_config, df_data_dict, distributor, 
                    df_neogrid_template, df_input)
            except Exception as error:
                print(error)
                sys.exit(1)

            try:
                print('processing_dates')
                dates, df_neogrid_template = processing_dates(
                    dists_individual_info_list, distributor,
                    df_neogrid_template, df_input)
            except Exception as error:
                print(error)
                sys.exit(1)
            
            try:
                print('filling_dates_into_neogrid_template')
                df_neogrid_template = filling_dates_into_neogrid_template(
                    df_neogrid_template, dates)
            except Exception as error:
                print(error)
                sys.exit(1)
            
            try:
                print('sanitizing_neogrid_template')
                sanitizing_neogrid_template(df_neogrid_template)
            except Exception as error:
                print(error)
                sys.exit(1)
                
            try:
                print('slicing_de_para_products')
                df_diageo_products, df_non_diageo_products, df_acessorios = slicing_de_para_products(df_de_para_products)
            except Exception as error:
                print(error)
                sys.exit(1)
            
            try:
                print('ean_validation')
                df_neogrid_template, df_new_products, df_acessorios_neogrid_template = ean_validation(distributor,
                    df_neogrid_template, df_diageo_products,
                    df_non_diageo_products, df_acessorios)
            except Exception as error:
                print(error)
                sys.exit(1)
            
            if len(df_acessorios_neogrid_template)>0:
                try:
                    print('assigning_columns_acessorios')
                    df_acessorios_neogrid_template = assigning_columns_acessorios(df_acessorios, df_acessorios_neogrid_template)
                except Exception as error:
                    print(error)
                    sys.exit(1)
            
                try:
                    print('writing_acessorios_file')
                    writing_acessorios_file(distributor, input_file_name, df_acessorios_neogrid_template, dates['time_stamp'])
                except Exception as error:
                    print(error)
                    sys.exit(1)

            if len(df_new_products) > 0:
                try:
                    print('writing_new_products_file')
                    writing_new_products_file(distributor, input_file_name,
                        df_new_products, dates['time_stamp'])
                except Exception as error:
                    print(error)
                    sys.exit()

            try:
                print('writing_neogrid_template_file')
                writing_neogrid_template_file(
                    df_neogrid_template, distributor, input_file_name, dates['time_stamp'])
            except Exception as error:
                print(error)
                sys.exit(1)

            try:
                print('moving_input_file_to_archive')
                moving_input_file_to_archive(input_file_name, dates['time_stamp'])
            except Exception as error:
                print(error)
                sys.exit(1)


            #Releasing memory before loading the next dataFrames
            gc.collect()
            df_input = pd.DataFrame()
            df_new_products = pd.DataFrame()
            df_acessorios_neogrid_template = pd.DataFrame()

            print('{} - Successfully executed!'.format(distributor))
    else:
        print('No distributor to be processed!')
    input('Press any key to close\n')

if __name__ == '__main__':
    main()

