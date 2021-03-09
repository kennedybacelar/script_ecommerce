import pandas as pd
from datetime import datetime

def loading_df_input(input_file_name, header, input_date_format, extra_arg):

    #Formating day to 2 digits
    year_and_month = extra_arg

    df_input = pd.read_excel(input_file_name, dtype=str,
        header=header, sheet_name='Plan1', encoding='mbcs').fillna('')

    columns_to_rows = df_input.columns[6:]
    fixed_columns = ['EAN', 'Descrição do Site']

    df_input = pd.melt(df_input, id_vars=fixed_columns, value_vars=columns_to_rows,
        var_name='Dia', value_name='QuantidadeTotal' 
    )

    df_input['Dia'] = year_and_month + df_input['Dia'].str.zfill(2)
    df_input['Dia'] = pd.to_datetime(df_input['Dia'], format=input_date_format, errors='raise')
    df_input.reset_index(drop=True, inplace=True)

    return df_input