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

def fix_query(data_path, file_name, new_file_name=None):
    if new_file_name is None:
        old_name, ext = file_name.split('.')
        new_file_name = f"{old_name}_new.{ext}"

    with open(data_path + file_name, 'r') as f:
        data_list = json.load(f)

    for data in data_list:
        query = data['query']
        query_for_parse = data['query_for_parse']
        # 人工添加统一的FROM子句，构成完整的SQL
        left, right = query.split('WHERE')
        left_for_parse, right_for_parse = query_for_parse.split('WHERE')
        table_str = 'table_' + data['db_id']
        new_query = f"{left}FROM _TABLE_ WHERE{right}"
        new_query_for_parse = f"{left_for_parse}FROM {table_str} WHERE{right_for_parse}"
        data['query'] = new_query_for_parse
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
    ## 先尝试不对content进行删减
    schema_list = []
    with open(schema_file, 'r') as f:
        schema_list = json.load(f)
    
    for schema in schema_list:
        if schema['db_id'] in db_id:
            new_column_names = []
            new_column_names_original = []
            new_column_type = []
            for idx in range(len(schema['column_names'])):
                if schema['column_names'][idx] not in new_column_names:
                    new_column_names.append(schema['column_names'][idx])
                    new_column_names_original.append(schema['column_names_original'][idx])
                    new_column_type.append(schema['column_types'][idx])
            schema['column_names'] = new_column_names
            schema['column_names_original'] = new_column_names_original
            schema['column_types'] = new_column_type
    
    with open(schema_file, 'w') as f:
        json.dump(schema_list, f, ensure_ascii=False)

if __name__ == '__main__':
    # fix_query('data/nl2sql/', 'train.json')
    # fix_query('data/nl2sql/', 'dev.json')
    # examine_schema()
    column_name_preprocess()