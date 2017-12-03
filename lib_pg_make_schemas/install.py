# -*- mode: python; coding: utf-8 -*-

import os, os.path
from . import descr

def install_cmd(args_ctx, print_func, err_print_func):
    if args_ctx.output is not None:
        raise NotImplementedError('feature ``args_ctx.settings_source_code`` is not implemented yet')
    
    if args_ctx.source_code is None:
        raise TypeError('rgs_ctx.source_code is None')
    
    if args_ctx.hosts is not None:
        hosts_path = os.path.realpath(args_ctx.hosts)
        hosts_descr = descr.HostsDescr()
        
        hosts_descr.load(hosts_path)
    else:
        hosts_descr = None
    
    source_code_path = os.path.realpath(args_ctx.source_code)
    source_code_descr = descr.ClusterDescr()
    
    source_code_descr.load(os.path.join(
        source_code_path,
        descr.ClusterDescr.file_name,
    ))
    
    if args_ctx.settings_source_code is not None:
        settings_source_code_path = os.path.realpath(args_ctx.settings_source_code)
        settings_source_code_descr = descr.ClusterDescr()
        
        settings_source_code_descr.load(os.path.join(
            settings_source_code_path,
            descr.ClusterDescr.file_name,
        ))
    else:
        settings_source_code_descr = None
    
    err_print_func('hosts_descr:', hosts_descr)
    err_print_func('source_code_descr:', vars(source_code_descr))
    err_print_func('source_code_descr.schemas_list[0]:', vars(source_code_descr.schemas_list[0]))
    err_print_func('source_code_descr.schemas_list[0].var_schema_list[0]:', vars(source_code_descr.schemas_list[0].var_schema_list[0]))
    err_print_func('source_code_descr.schemas_list[0].func_schema_list[0]:', vars(source_code_descr.schemas_list[0].func_schema_list[0]))
    err_print_func('settings_source_code_descr:', settings_source_code_descr)
