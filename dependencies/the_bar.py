import pandas as pd 

def loading_df_input(input_file_name, header, input_date_format, extra_arg):

    """
    In order to group by the needed columns to avoid duplicate rows, some others had to be discarded
    So, if that function is used, the only columns that can be assigned in the data dictionary
    Are the remaining 6 columns below 
    """

    df_input = pd.read_excel(input_file_name, dtype=str,
        header=header).fillna('')
    
    df_input = df_input[[
        'Pedido - Dat. de cadastro', 'CPF/CNPJ (sem máscara)', 'Item - Cód. Barras',
        'Item - Nome composto', 'Item - Qtde. Faturada', 'Item - Valor líquido'
    ]]

    df_input['Item - Qtde. Faturada'] = pd.to_numeric(df_input['Item - Qtde. Faturada'], errors='coerce').fillna(0)
    df_input['Item - Valor líquido'] = pd.to_numeric(df_input['Item - Valor líquido'], errors='coerce').fillna(0)

    df_input['Pedido - Dat. de cadastro'] = pd.to_datetime(df_input['Pedido - Dat. de cadastro'], format=input_date_format, errors='raise')

    df_input = df_input.groupby([
        'Pedido - Dat. de cadastro', 'CPF/CNPJ (sem máscara)',
        'Item - Cód. Barras', 'Item - Nome composto'
    ]).sum()

    df_input.reset_index(inplace=True)

    return df_input

