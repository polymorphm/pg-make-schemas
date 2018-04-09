# -*- mode: python; coding: utf-8 -*-

def read_settings_sql(cluster_descr, host_type):
    for settings_descr in cluster_descr.settings_list:
        if settings_descr.settings_type != host_type:
            continue
        
        yield from settings_descr.read_sql()
