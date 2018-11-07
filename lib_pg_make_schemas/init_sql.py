def read_init_sql(cluster_descr, host_type):
    for schemas_descr in cluster_descr.schemas_list:
        if schemas_descr.schemas_type != host_type:
            continue

        init_descr = schemas_descr.init

        if init_descr is None:
            continue

        yield from init_descr.read_sql()

# vi:ts=4:sw=4:et
