import pandas as pd 
from datetime import date, datetime

def loading_df_input(input_file_name, header, extra_arg):

    #dateparse = lambda x: datetime.strftime(x, '%m/%d/%Y')
    #df_input = pd.read_excel(input_file_name, dtype=str, date_parser=dateparse).fillna('')
    df_input = pd.read_excel(input_file_name).fillna('')
    df_input['DIA'] = df_input['DIA'].dt.strftime('%m/%d/%Y')

    dar = df_input.loc[0, 'DIA']
    print(dar)
    print('ken')
    print(type(dar))
    exit()
    #parse_dates=['DIA']

    return df_input