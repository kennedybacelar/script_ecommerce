import pandas as pd     
import os
import sys
import warnings
from datetime import datetime
import json

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
pd.options.mode.chained_assignment = None

def setting_config_paths():

    dist_info_path = '../distributors_info.xlsx'
    dist_config_path = '../distributors_config.xlsx'
    data_dictionary_path = '../data_dictionary.xlsx'

    return dist_info_path, dist_config_path, data_dictionary_path


def loading_config_information(dist_info_path, dist_config_path, data_dictionary_path):

    df_dist_info = pd.read_excel(dist_info_path, dtype=str, header=0).fillna('')
    df_dist_config = pd.read_excel(open(dist_config_path, 'rb'), dtype=str, header=0).fillna('')
    df_data_dict = pd.read_excel(data_dictionary_path, dtype=str, header=0).fillna('')
    
    return df_dist_info, df_dist_config, df_data_dict


def loading_neogrid_template():

    neogrid_file_path_template = '../TEMPLATE Neogrid.xlsx'
    df_neogrid_template = pd.read_excel(neogrid_file_path_template, dtype=str, header=0).fillna('')

    return df_neogrid_template


def sanitizing_config_file(df_dist_info, df_dist_config):

    columns = df_dist_config.columns

    for single_column in columns:
        df_dist_config[single_column] = df_dist_config[single_column].str.strip()
    
    df_dist_config['to_be_processed'] = df_dist_config['to_be_processed'].str.lower()

    #Dropping distributors flagged not to be processed
    to_be_dropped = df_dist_config[df_dist_config['to_be_processed'] != 'y'].index
    df_dist_config.drop(to_be_dropped, inplace=True)

    return df_dist_info, df_dist_config


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


def loading_input_file(folder_name, header):

    header = int(header)
    input_directory = '../' + folder_name

    if not os.path.isdir(input_directory):
        print('Folder does not exist - {}'.format(input_directory))
        sys.exit(1)

    #Checking if there is exactly one file in the referred directory
    if len(os.listdir(input_directory)) > 1:
        print('Error: More than one input file to be processed - {}'.format(folder_name))
        sys.exit(1)
    elif (len(os.listdir(input_directory)) == 0):
        print('No files in {}'.format(folder_name))
        sys.exit(1)
    else:
        input_file_name = input_directory + '/' + os.listdir(input_directory)[0]
        df_input = pd.read_excel(input_file_name, dtype=str, header=header).fillna('')
        
    return df_input


def assigning_columns(df_dist_config, df_data_dict, distributor, df_neogrid_template, df_input):

    #Creating list of keys of dict
    columns_to_be_checked = df_dist_config.columns[6:]
    df_data_dict.set_index(['Neogrid_template'], inplace=True)

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
            print('{}- Column not present in input file'.format(error))
    

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

    if dists_individual_info_list[distributor]['date']:
        ano = dists_individual_info_list[distributor]['date'][:4]
        month = dists_individual_info_list[distributor]['date'][4:6]
        day = dists_individual_info_list[distributor]['date']

        trim = ano + ' Trimestre ' + declaring_de_para_dates(month)[0]
        month = ano + ' ' + declaring_de_para_dates(month)[1]

        print(day)
        print(trim)
        print(month)





dist_info_path, dist_config_path, data_dictionary_path = setting_config_paths()
df_dist_info, df_dist_config, df_data_dict = loading_config_information(dist_info_path, dist_config_path, data_dictionary_path)
df_neogrid_template = loading_neogrid_template()
df_dist_info, df_dist_config = sanitizing_config_file(df_dist_info, df_dist_config)
dists_individual_info_list = filtering_config_info(df_dist_config)
df_input = loading_input_file(dists_individual_info_list['Amazon']['folder_name'], dists_individual_info_list['Amazon']['header'])

distributor = 'Amazon'
df_neogrid_template = assigning_columns(df_dist_config, df_data_dict, distributor, df_neogrid_template, df_input)
processing_dates(dists_individual_info_list, distributor, df_neogrid_template, df_input)

exit()
print(df_neogrid_template)


"""
Defining Paths
Load Data Frames
Sanitizing DFS
Declaring Neogrid Template
"""