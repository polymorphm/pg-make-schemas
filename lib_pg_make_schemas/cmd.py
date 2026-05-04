def var_schemas(cluster_descr, host_type):
    schemas = []

    for schemas_descr in cluster_descr.schemas_list:
        if schemas_descr.schemas_type != host_type:
            continue

        for schema_descr in schemas_descr.var_schema_list:
            schemas.append(schema_descr.schema_name)

    return schemas

def schemas_role(cluster_descr, host_type):
    for schemas_descr in cluster_descr.schemas_list:
        if schemas_descr.schemas_type != host_type:
            continue

        return schemas_descr.role

    return cluster_descr.role

def settings_role(cluster_descr, host_type):
    for settings_descr in cluster_descr.settings_list:
        if settings_descr.settings_type != host_type:
            continue

        return settings_descr.role

    return cluster_descr.role

def func_schemas(cluster_descr, host_type):
    schemas = []

    for schemas_descr in cluster_descr.schemas_list:
        if schemas_descr.schemas_type != host_type:
            continue

        for schema_descr in schemas_descr.func_schema_list:
            schemas.append(schema_descr.schema_name)

    return schemas

def is_single_host_run(hosts_descr):
    return len(hosts_descr.host_list) == 1

def filter_host_name(hosts_descr, host_name):
    if host_name is None:
        return

    for host in hosts_descr.host_list:
        if host['name'] != host_name:
            continue

        hosts_descr.host_list = [host]

        return

    raise ValueError('{!r}: host_name is not found'.format(host_name))

def update_hosts_shared(hosts_descr, define_map):
    if not define_map:
        return

    if hosts_descr.shared is None:
        hosts_descr.shared = {}

    if not isinstance(hosts_descr.shared, dict):
        raise ValueError('not isinstance(hosts_descr.shared, dict)')

    hosts_descr.shared.update(define_map)

def update_conninfo(hosts_descr, conninfo):
    if conninfo is None:
        return

    if not is_single_host_run(hosts_descr):
        raise ValueError('unable to use --conninfo without a single-host run')

    hosts_descr.host_list[0]['conninfo'] = conninfo

def update_host_params(hosts_descr, host_define_map):
    if not host_define_map:
        return

    if not is_single_host_run(hosts_descr):
        raise ValueError('unable to use --host-define without a single-host run')

    host = hosts_descr.host_list[0]

    if host['params'] is None:
        host['params'] = {}

    host['params'].update(host_define_map)

def apply_hosts_options(
        hosts_descr,
        host_name,
        define_map,
        conninfo,
        host_define_map):
    update_hosts_shared(hosts_descr, define_map)
    filter_host_name(hosts_descr, host_name)
    update_conninfo(hosts_descr, conninfo)
    update_host_params(hosts_descr, host_define_map)

# vi:ts=4:sw=4:et
