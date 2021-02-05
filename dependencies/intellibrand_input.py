import pandas as pd

def loading_df_input(input_file_name, header, extra_arg):

    with open(input_file_name) as f:
        input_file = list(f.readlines()[header:])

        # replace NUL, strip whitespace from the end of the strings, split each string into a list
        input_file = [v.replace('\x00', '').strip().split(';') for v in input_file]

        # remove some empty rows
        input_file = [v for v in input_file if len(v) > 2]

    # load the file with pandas
    df_input = pd.DataFrame(input_file, dtype=str)

    df_input.columns = df_input.loc[0]

    #This command is because the function open is creating an extra row with numeric indexes
    df_input.drop(df_input.index[0], inplace=True)
    df_input.reset_index(drop=True, inplace=True)

    return df_input

