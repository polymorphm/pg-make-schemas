def var_schemas(cluster_descr, host_type):
    schemas = []

    for schemas_descr in cluster_descr.schemas_list:
        if schemas_descr.schemas_type != host_type:
            continue

        for schema_descr in schemas_descr.var_schema_list:
            schemas.append(schema_descr.schema_name)

    return schemas

def func_schemas(cluster_descr, host_type):
    schemas = []

    for schemas_descr in cluster_descr.schemas_list:
        if schemas_descr.schemas_type != host_type:
            continue

        for schema_descr in schemas_descr.func_schema_list:
            schemas.append(schema_descr.schema_name)

    return schemas

# vi:ts=4:sw=4:et
