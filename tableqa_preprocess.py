"""
1. 根据TableQA数据集的query字段还原出目标SQL语句 -> 需要添加FROM
2. 根据TableQA数据集的tables.json还原出db_schema.json和db_content.json
"""
"""
    处理NL2SQL的方法:
    1. 强行指定一个节点__Table__, 在选择DB_CONSTANT时无论是训练还是推理都将其分数置为最大
    2. 修改规则，与R相关的计算全部去掉，新增特殊的投影运算: C X P -> R
        2较难实现，因为需要实现一套特殊语法
"""
import json
from src.utils.generate_query_toks import generate_query_toks
from src.eval.dusql.dusql_evaluation import get_complex_sql_json

def fix_query(data_path, file_name, new_file_name=None):
    if new_file_name is None:
        old_name, ext = file_name.split('.')
        new_file_name = f"{old_name}_new.{ext}"

    with open(data_path + file_name, 'r') as f:
        data_list = json.load(f)

    for data in data_list:
        if 'FROM' not in data['query'] or 'from' not in data['query']:
            query = data['query']
            # query_for_parse = data['query_for_parse']
            # 人工添加统一的FROM子句，构成完整的SQL
            left, right = query.split('WHERE')
            # left_for_parse, right_for_parse = query_for_parse.split('WHERE')
            table_str = 'table_' + data['db_id']
            new_query = f"{left}FROM {table_str} WHERE{right}"
            # new_query_for_parse = f"{left_for_parse}FROM {table_str} WHERE{right_for_parse}"
            data['query'] = new_query
            # data['query_for_parse'] = new_query_for_parse
    
    with open(data_path + file_name, 'w') as f:
        json.dump(data_list, f, ensure_ascii=False)

def examine_schema(schema_file='data/nl2sql/db_schema.json'):
    schema_list = []
    with open(schema_file, 'r') as f:
        schema_list = json.load(f)
    
    db_id_set = set()
    for schema in schema_list:
        name_set = set()
        for column_name in schema['column_names']:
            if column_name[1] in name_set:
                db_id_set.add(schema['db_id'])
                break
            else:
                name_set.add(column_name[1])
    
    for db_id in db_id_set:
        print(db_id)

def column_name_preprocess(schema_file='data/nl2sql/db_schema.json', content_file='data/nl2sql/db_content.json'):
    ## 去掉重复的列名和列值
    db_id = [
        "43ad7ef81d7111e99933f40f24344a08",
        "43ae62591d7111e9a3f6f40f24344a08",
        "43af253d1d7111e9a7ddf40f24344a08",
        "43ae7f9c1d7111e9bc7af40f24344a08",
        "43b1adb51d7111e98489f40f24344a08",
        "43b34d401d7111e9b6d5f40f24344a08",
    ]
    schema_list = []
    with open(schema_file, 'r') as f:
        schema_list = json.load(f)
    
    column_name_set = set()
    for schema in schema_list:
        if schema['db_id'] in db_id:
            # new_column_type = []
            duplicate_count = 0
            for idx in range(len(schema['column_names'])):
                if schema['column_names'][idx][1] not in column_name_set:
                    column_name_set.add(schema['column_names'][idx][1])
                else:
                    duplicate_count += 1
                    schema['column_names'][idx][1] = f"{schema['column_names'][idx][1]}::{str(duplicate_count)}"
                    schema['column_names_original'][idx][1] = f"{schema['column_names_original'][idx][1]}::{str(duplicate_count)}"
    with open(schema_file, 'w') as f:
        json.dump(schema_list, f, ensure_ascii=False)

def generate_complex_sql_json(data_file, schema_file):
    data_list = []
    with open(data_file, 'r') as f:
        data_list = json.load(f)
    
    for data in data_list:
        sql_json = get_complex_sql_json(data['query'], schema_file, data['db_id'])
        data['sql'] = sql_json

    with open(data_file, 'w') as f:
        json.dump(data_list, f, ensure_ascii=False)

def generate_query_toks_nl2sql(file_name):
    data_list = []
    with open(file_name, 'r') as f:
        data_list = json.load(f)
    
    for data in data_list:
        data['query'] = data['query'].replace('==', '=')
        data['query_toks'] = data['query'].split(' ')
    
    with open(file_name, 'w') as f:
        json.dump(data_list, f, ensure_ascii=False)

if __name__ == '__main__':
    # generate_query_toks_nl2sql('data/nl2sql/train.json')
    # generate_query_toks_nl2sql('data/nl2sql/dev.json')
    # fix_query('data/nl2sql/', 'train.json')
    # fix_query('data/nl2sql/', 'dev.json')
    # generate_complex_sql_json('data/nl2sql/train.json', 'data/nl2sql/db_schema.json')
    # generate_complex_sql_json('data/nl2sql/dev.json', 'data/nl2sql/db_schema.json')
    examine_schema()
    column_name_preprocess()