import pathlib
import pandas as pd
import argparse
import re
from itertools import zip_longest

# Import REGEX Clean SQL File
from regex_clean import clean_sql

report_columns = [
    '檔案名稱',
    'Source Operator',
    'Source Line',
    'Source Table',
    'Target Operator',
    'Target Line',
    'Target Table',
]
target_column = ['FileName', 'Operator', 'Target_Lines', 'Target_Table']

source_column = ['FileName', 'Operator', 'Source_Lines', 'Source_Table']


# 先組合 並拆分成 Source / Target


def extract_table(_file):
    # 拆分 target / source

    target_list = []
    source_list = []

    sql_line = clean_sql(_file)

    for i, line in enumerate(sql_line):
        # Target 複製下來 但 report_list 改成 target_list

        # Target 邏輯盤點

        line = line.upper()

        # 1. Drop Table
        if re.search(r'\bDROP TABLE IF EXISTS\b', line, flags=re.I):
            # 去除前置空白 以及 DROP TABLE IF EXISTS 前的多餘字串
            mess = line[line.find("DROP TABLE IF EXISTS"):].replace(";", "").lstrip()

            if len(mess.split()) <= 4:
                # 以空白切割字串
                # 若只有 DROP TABLE IF EXISTS 那就只有 4 個字 故長度為 4
                continue

            elif len(mess.split()) > 4:
                print("TARGET:DROP TABLE IF EXISTS : " + mess.split()[4].strip())
                # 第 5 個字詞位置 為 Table Name
                _row = [str(_file), "TARGET:DROP TABLE IF EXISTS", i + 1, mess.split()[4].strip()]
                target_list.append(_row)

            else:
                continue

        elif re.search(r'\bDROP TABLE\b', line, flags=re.I):
            mess = line[line.find("DROP TABLE"):].replace(";", "").lstrip()
            if len(mess.split()) <= 2:
                continue
            elif len(mess.split()) > 2:
                print("TARGET:DROP TABLE : " + mess.split()[2].strip())

                _row = [str(_file), "TARGET:DROP TABLE", i + 1, mess.split()[2].strip()]
                target_list.append(_row)
            else:
                continue

        # 2. Create Table
        elif re.search(r'\bCREATE TABLE\b', line, flags=re.I):
            mess = line[line.find("CREATE TABLE"):].replace(";", "").lstrip()
            if len(mess.split()) <= 2:
                continue
            elif len(mess.split()) > 2:
                print("TARGET:CREATE TABLE : " + mess.split()[2].strip())
                _row = [str(_file), "TARGET:CREATE TABLE", i + 1, mess.split()[2].strip()]
                target_list.append(_row)
            else:
                continue

        # 3. Delete Table
        # 要注意的是 DELETE FROM 要先判斷
        elif re.search(r'\bDELETE FROM\b', line, flags=re.I):
            mess = line[line.find("DELETE FROM"):].replace(";", "").lstrip()
            if len(mess.split()) <= 2:
                continue
            elif len(mess.split()) > 2:
                print("TARGET:DELETE FROM : " + mess.split()[2].strip())
                _row = [str(_file), "TARGET:DELETE FROM", i + 1, mess.split()[2].strip()]
                target_list.append(_row)

        #  之後才判斷 DELETE TABLE NOT FROM (TD Only)
        elif re.search(r'\bDELETE\b', line):
            mess = line[line.find("DELETE"):].replace(";", "").lstrip()
            if len(mess.split()) <= 1:
                continue
            elif len(mess.split()) > 1:
                print("TARGET:DELETE : " + mess.split()[1].strip())
                _row = [str(_file), "TARGET:DELETE", i + 1, mess.split()[1].strip()]
                target_list.append(_row)

        # 4. Insert & 剩餘其他
        elif re.search(r'\bINSERT INTO\b', line, flags=re.I):
            mess = line[line.find("INSERT INTO"):].replace(";", "").lstrip()
            if len(mess.split()) <= 2:
                continue
            elif len(mess.split()) > 2:
                print("TARGET:INSERT INTO : " + mess.split()[2])
                _row = [str(_file), "TARGET:INSERT INTO", i + 1, mess.split()[2].strip()]
                target_list.append(_row)
            else:
                continue

        # TRUNCATE TABLE
        elif re.search(r'\bTRUNCATE TABLE\b', line, flags=re.I):
            mess = line[line.find("TRUNCATE TABLE"):].replace(";", "").lstrip()
            if len(mess.split()) <= 2:
                continue
            elif len(mess.split()) > 2:
                print("TARGET:TRUNCATE TABLE : " + mess.split()[2].strip())
                _row = [str(_file), "TARGET:TRUNCATE TABLE", i + 1, mess.split()[2].strip()]
                target_list.append(_row)

            else:
                continue

        # REPLACE VIEW
        elif re.search(r'\bREPLACE VIEW\b', line, flags=re.I):
            mess = line[line.find("REPLACE VIEW"):].replace(";", "").lstrip()
            if len(mess.split()) <= 2:
                continue
            elif len(mess.split()) > 2:
                print("TARGET:REPLACE VIEW : " + mess.split()[2].strip())
                _row = [str(_file), "TARGET:REPLACE VIEW", i + 1, mess.split()[2].strip()]
                target_list.append(_row)

            else:
                continue

        # Source 部分 複製下來 report 改成 source_list
        # Source 邏輯盤點
        # FROM
        if re.search(r'\bFROM\b', line, flags=re.I):

            if re.search(r"""['"].*?from.*?['"]""", line, flags=re.I):
                # 判斷 FROM 是否出現在 ' ' 之中 或 " " 之中
                continue

            start_regex_index = re.search(r'\bFROM\b', line, flags=re.I).start()
            mess = line[start_regex_index:].replace(";", "").lstrip()
            if len(mess.split()) == 1:
                # 單行只寫一個 FROM  --> 跳過
                continue
            elif mess.split()[1].startswith('('):
                # FROM SubQuery  --> 跳過
                continue
                #       FROM 內的 inner join 處理
            elif len(mess.split(",")) > 1:
                for element in mess.split(","):
                    # 多個 INNER JOIN Table 判斷
                    element = element.lstrip()
                    if element:
                        element = element.replace("FROM", "").lstrip()
                        ele_check = element.split()
                        if len(ele_check) == 0:
                            continue
                        element = element.split()[0].replace(";", "").lstrip()
                        print("SOURCE:FROM : " + element.strip())
                        _row = [str(_file), "SOURCE:FROM", i + 1, element.strip()]
                        source_list.append(_row)
            elif len(mess.split()) > 1:
                print("SOURCE:FROM : " + mess.split()[1].strip())
                _row = [str(_file), "SOURCE:FROM", i + 1, mess.split()[1].strip()]
                source_list.append(_row)
            else:
                continue

        # JOIN
        elif re.search(r'\bJOIN\b', line, flags=re.I):
            #             print('Line : {}'.format(i + 1))
            start_regex_index = re.search(r'\bJOIN\b', line, flags=re.I).start()
            mess = line[start_regex_index:].replace(";", "").lstrip()
            if len(mess.split()) == 1:
                continue
            elif mess.split()[1].startswith('('):
                continue
            elif len(mess.split()) > 1:
                if re.search('lateral', mess.split()[1].strip(), re.I):
                    continue
                print("SOURCE:JOIN : " + mess.split()[1].strip())
                _row = [str(_file), "SOURCE:JOIN", i + 1, mess.split()[1].strip()]
                source_list.append(_row)

            else:
                continue

    # 同時 return Target / Source
    return target_list, source_list


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-S', help='Single File Mode')
    parser.add_argument('-P', help='File Path Single or Folder')
    parser.add_argument('--pattern', help='File Pattern .Not Work in Single File Mode')
    parser.add_argument('--exclude', action='append', help='Exclude File pattern. Using Multiple '
                                                           '--exclude= to Exclude Multi Pattern')
    args = parser.parse_args()
    # 匯出報表檔名
    output_file_name = 'Output.xlsx'

    # 匯出報表路徑
    if not args.S:

        output_path = './'

        # Script 檔案所在資料夾
        if not args.P:
            path = 'Script/'
        else:
            print("Using Custom Path {}".format(args.P))
            path = args.P

        # File Pattern
        if not args.pattern:
            file_pattern = '*.sql'
        else:
            print("Using Custom Pattern {}".format(args.pattern))
            file_pattern = args.pattern

        # 是否抓取 資料夾裡的 子資料夾
        recursive = True

        # 是否要印出資訊
        silent = True

        output_path = pathlib.Path(output_path)

        if not output_path.exists():
            output_path.mkdir()

        output_file_name = output_path / output_file_name

        # Pathlib Include Windows Path & Unix Path
        p = pathlib.Path(path)
        # Check File Exists
        if not p.exists():
            raise RuntimeError('ＷＴＦ 檔案呢？')

        if recursive:
            file_list = p.rglob(file_pattern)
        #     file_pattern = '**/' + file_pattern

        else:

            file_list = p.glob(file_pattern)

        file_list = [f for f in file_list if not (f.name.startswith('.') or f.name.startswith('Vi@@BK@@_') or
                                                  str(f.parent).lower().endswith('bk') or
                                                  str(f.parent).lower().endswith('backup'))]

    else:
        file_list = [pathlib.Path(args.P)]

    exclude_mode = False
    if args.exclude:
        exclude_mode = True
    else:
        exclude_pattern = []

    if len(file_list) == 0:
        print('No File To Scan ! Plz Check')
        exit(0)

    report_list = []
    for file in file_list:

        bypass = False
        if exclude_mode:

            for i in args.exclude:
                if re.search(i, file.name):
                    bypass = True

        if bypass:
            continue
        print('=============== 現在處理 {}'.format(file.name))

        target, source = extract_table(file)

        # 去重複 - Target

        df = pd.DataFrame(target, columns=target_column).drop_duplicates(subset=['Target_Table'])

        # 重新組合 去重後的 row
        re_target = []

        # pandas.DataFrame.iterrows --> 回傳 index 跟 row
        for index, row in df.iterrows():
            re_target.append(row)

        # 去重複 - Source

        df = pd.DataFrame(source, columns=source_column).drop_duplicates(subset=['Source_Table'])

        re_source = []

        for index, row in df.iterrows():
            re_source.append(row)

        for _target, _source in zip_longest(re_target, re_source):

            row = [str(file)]
            if _source is not None:
                # extend 可直接做 list 合併
                row.extend([_source[1], _source[2], _source[3]])

            # 若 source 較少時 會回傳 None
            else:
                # 都是 None
                row.extend([_source, _source, _source])

            if _target is not None:
                row.extend([_target[1], _target[2], _target[3]])

            # 若 target 較少時 會回傳 None
            else:
                row.extend([_target, _target, _target])

            report_list.append(row)

    pd.DataFrame(report_list, columns=report_columns).to_excel(output_file_name, index=False)
