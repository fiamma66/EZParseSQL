import re


def clean_sql(file):
    out_encode = None

    # 讀檔
    try:
        with open(file, encoding='utf-8') as f:

            sql = f.read()
        out_encode = 'utf-8'
    except UnicodeDecodeError:
        try:
            with open(file, encoding='ms950') as f:
                sql = f.read()
            out_encode = 'ms950'
        except UnicodeDecodeError:
            with open(file, encoding='utf-8', errors='surrogateescape') as f:
                sql = f.read()
            out_encode = 'utf-8'

    # clean /* */ 註解
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL | re.MULTILINE)

    # clean -- 註解
    sql = re.sub(r'--.*\n*|(.*?)--.*\n*', r'\g<1>\n', sql)

    # 第二點的 keyword 延伸
    sp_function_keyword = [
        'TRIM',
        'EXTRACT',
        'SUBSTRING',
    ]
    # 關鍵字的地方 給他一個 {} 來讓 字串填入
    sp_function_pattern = r'(\b{}\b *\(.*?)\bfrom\b(.*?\))'
    # List Comprehension
    regex_sp_function = [re.compile(sp_function_pattern.format(kw), flags=re.I) for kw in sp_function_keyword]

    for regex in regex_sp_function:
        # 不需要 flags 了 前面已經 compile 過 且加入 flags 了
        sql = re.sub(regex, r'\g<1> \g<2>', sql)

    # 第三點的 keyword 延伸
    sql_kw = [
        'UPDATE',
        'DELETE',
        'FROM',
        'REPLACE VIEW',
        'JOIN',
        'INSERT INTO',
        'DROP',
        'DROP TABLE',
        'CREATE',
    ]
    sql_kw_pattern = r'(\b{}\b *)\n+'
    regex_sql = [re.compile(sql_kw_pattern.format(kw), flags=re.I) for kw in sql_kw]

    for regex in regex_sql:
        sql = re.sub(regex, r'\g<1>', sql)

    sub_query_pattern = r'(\b{}\b.*?)([()].*)'

    sub_query_kw = [
        'FROM',
        'JOIN'
    ]

    compile_sub_query = [re.compile(sub_query_pattern.format(kw), flags=re.I) for kw in sub_query_kw]

    for pattern in compile_sub_query:
        sql = re.sub(pattern, r'\g<1>\n\g<2>', sql)

    # clean 多餘空白 -- 最後修正部分判定
    sql = re.sub(r' +', ' ', sql)

    # 額外寫檔出來 我們都寫進 子資料夾 bk b_打頭
    b_file = file.parent / 'bk' / ('b_' + file.name)

    if not b_file.parent.exists():
        b_file.parent.mkdir()

    with open(b_file, 'w', encoding=out_encode, errors='surrogateescape') as f:
        f.write(sql)

    return sql.split('\n')
