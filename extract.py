import pandas as pd


df = pd.read_csv('VIEW.csv')

if __name__ == '__main__':

    for index, value in df.iterrows():

        prop_name = value[0]
        prop_name = prop_name + '.prop'
        content = value[1]

        with open(prop_name, 'w') as f:
            f.write('ORDERBY={}'.format(content))
            f.write('\n')

            f.write('EXCLUDE_COLUMNS=EDW_UPD_DT \n')
            f.write('TIME_COLUMNS=\n')
            f.write('CONDITION= \n')

