# -*- mode: python; coding: utf-8 -*-

import os, os.path
import contextlib
from . import verbose
from . import descr
from . import settings
from . import revision_sql
from . import comment
from . import receivers
from . import install
from . import pg_role_path
from . import scr_env
from . import upgrade
from . import init_sql
from . import install_sql
from . import upgrade_sql
from . import safeguard_sql

class UpgradeCmdError(Exception):
    pass

def upgrade_cmd(args_ctx, print_func, err_print_func):
    if args_ctx.rev is None and not args_ctx.execute:
        raise UpgradeCmdError('unable to upgrade without any information about revision')
    
    verb = verbose.make_verbose(print_func, err_print_func, args_ctx.verbose)
    
    verb.prepare_upgrade()
    
    hosts_descr = descr.HostsDescr()
    
    if args_ctx.hosts is not None:
        hosts_path = os.path.realpath(args_ctx.hosts)
        
        hosts_descr.load(hosts_path)
    
    include_list = []
    
    for include in args_ctx.include_list:
        include_list.append(os.path.realpath(include))
    
    source_code_file_path = os.path.realpath(os.path.join(
        args_ctx.source_code,
        descr.ClusterDescr.file_name,
    ))
    source_code_include_list = include_list + [os.path.dirname(source_code_file_path)]
    source_code_cluster_descr = descr.ClusterDescr()
    
    source_code_cluster_descr.load(source_code_file_path, source_code_include_list)
    
    if args_ctx.hosts is None:
        hosts_descr.load_pseudo(source_code_cluster_descr)
    
    rev_sql = revision_sql.RevisionSql(source_code_cluster_descr.application)
    
    settings_cluster_descr_list = []
    
    if args_ctx.comment:
        if args_ctx.comment_path is not None:
            comment_file_path = args_ctx.comment_path
        else:
            comment_file_path = os.path.realpath(os.path.join(
                args_ctx.source_code,
                comment.COMMENT_FILE_NAME,
            ))
        
        com = comment.comment(comment_file_path)
    else:
        com = None
    
    for settings_source_code in args_ctx.settings_source_code:
        settings_file_path = os.path.realpath(os.path.join(
            settings_source_code,
            descr.ClusterDescr.file_name,
        ))
        settings_include_list = include_list + [os.path.dirname(settings_file_path)]
        settings_cluster_descr = descr.ClusterDescr()
        
        settings_cluster_descr.load(
            settings_file_path,
            settings_include_list,
            settingsMode=True,
        )
        
        settings.check_settings_compatibility(
            source_code_cluster_descr,
            settings_cluster_descr,
        )
        
        settings_cluster_descr_list.append(settings_cluster_descr)
    
    var_rev_map = {}
    var_com_map = {}
    func_rev_map = {}
    func_com_map = {}
    migr_list_map = {}
    
    verb.source_code_revision(
        source_code_cluster_descr.application,
        source_code_cluster_descr.revision,
        com,
    )
    
    with contextlib.closing(
                receivers.Receivers(
                    args_ctx.execute,
                    args_ctx.pretend,
                    args_ctx.output,
                ),
            ) as recv:
        recv.begin(hosts_descr, begin_host_verb_func=verb.begin_host)
        
        for host in hosts_descr.host_list:
            host_name = host['name']
            host_type = host['type']
            
            func_schemas = install.func_schemas(source_code_cluster_descr, host_type)
            
            recv.execute(host_name, pg_role_path.pg_role_path('postgres', None))
            
            verb.scr_env(host_name)
            
            recv.execute(host_name, scr_env.scr_env(hosts_descr, host_name))
            
            verb.ensure_revision_structs(host_name)
            
            recv.execute(host_name, rev_sql.ensure_revision_structs())
            
            if args_ctx.rev is not None:
                host_var_rev, host_var_com = args_ctx.rev, None
                host_func_rev, host_func_com = args_ctx.rev, None
            else:
                host_var_rev, host_var_com = rev_sql.fetch_var_revision(recv, host_name)
                host_func_rev, host_func_com = rev_sql.fetch_func_revision(recv, host_name)
                
                upgrade.print_revision(
                    host_name,
                    host_type,
                    host_var_rev,
                    host_var_com,
                    host_func_rev,
                    host_func_com,
                    print_func,
                )
            
            host_migr_list = upgrade.find_migr_way(
                source_code_cluster_descr,
                host_type,
                host_var_rev,
            )
            
            upgrade.print_migr_way(
                host_name,
                host_type,
                host_migr_list,
                print_func,
            )
            
            verb.guard_var_revision(host_name, host_var_rev)
            
            recv.execute(host_name, rev_sql.guard_var_revision(host_var_rev))
            
            if not args_ctx.show_rev:
                if not args_ctx.change_rev:
                    verb.drop_func_schemas(host_name)
                    
                    recv.execute(host_name, rev_sql.drop_func_schemas(func_schemas))
                
                verb.arch_var_revision(host_name)
                
                recv.execute(host_name, rev_sql.arch_var_revision())
                
                verb.arch_func_revision(host_name)
                
                recv.execute(host_name, rev_sql.arch_func_revision())
            
            var_rev_map[host_name] = host_var_rev
            var_com_map[host_name] = host_var_com
            func_rev_map[host_name] = host_func_rev
            func_com_map[host_name] = host_func_com
            migr_list_map[host_name] = host_migr_list
        
        if not args_ctx.show_rev:
            if not args_ctx.change_rev:
                for host in hosts_descr.host_list:
                    host_name = host['name']
                    
                    host_migr_list = migr_list_map[host_name]
                    
                    if host_migr_list is None:
                        raise UpgradeCmdError(
                            '{!r}: there is no way to do migration'.format(
                                host_name,
                            ),
                        )
                
                if args_ctx.init:
                    for host in hosts_descr.host_list:
                        host_name = host['name']
                        host_type = host['type']
                        
                        for i, sql in enumerate(
                                    init_sql.read_init_sql(source_code_cluster_descr, host_type),
                                ):
                            if not i:
                                verb.execute_sql(host_name, 'init_sql')
                            
                            recv.execute(
                                host_name, '{}\n\n{}\n\n;'.format(
                                    pg_role_path.pg_role_path('postgres', None),
                                    sql.rstrip(),
                                ),
                            )
                
                for host in hosts_descr.host_list:
                    host_name = host['name']
                    host_type = host['type']
                    
                    host_migr_list = migr_list_map[host_name]
                    interm_migr_list, final_migr_list = host_migr_list[:-1], host_migr_list[-1:]
                    
                    if interm_migr_list:
                        for interm_migr in interm_migr_list:
                            for i, sql in enumerate(
                                        upgrade_sql.read_upgrade_sql(
                                            source_code_cluster_descr,
                                            host_type,
                                            interm_migr,
                                        ),
                                    ):
                                if not i:
                                    verb.execute_sql(host_name, 'upgrade_sql')
                                
                                recv.execute(
                                    host_name, '{}\n\n{}\n\n;'.format(
                                        pg_role_path.pg_role_path('postgres', None),
                                        sql.rstrip(),
                                    ),
                                )
                            
                            for settings_cluster_descr in settings_cluster_descr_list:
                                for i, sql in enumerate(
                                            upgrade_sql.read_upgrade_sql(
                                                settings_cluster_descr,
                                                host_type,
                                                interm_migr,
                                            ),
                                        ):
                                    if not i:
                                        verb.execute_sql(host_name, 'settings_upgrade_sql')
                                    
                                    recv.execute(
                                        host_name, '{}\n\n{}\n\n;'.format(
                                            pg_role_path.pg_role_path('postgres', None),
                                            sql.rstrip(),
                                        ),
                                    )
                            
                            verb.push_var_revision(host_name, interm_migr[0], None)
                            
                            recv.execute(
                                host_name,
                                rev_sql.push_var_revision(interm_migr[0], None, None),
                            )
                            
                            verb.arch_var_revision(host_name)
                            
                            recv.execute(host_name, rev_sql.arch_var_revision())
                    
                    if final_migr_list:
                        final_migr = final_migr_list[0]
                        
                        for i, sql in enumerate(
                                    upgrade_sql.read_upgrade_sql(
                                        source_code_cluster_descr,
                                        host_type,
                                        final_migr,
                                    ),
                                ):
                            if not i:
                                verb.execute_sql(host_name, 'upgrade_sql')
                            
                            recv.execute(
                                host_name, '{}\n\n{}\n\n;'.format(
                                    pg_role_path.pg_role_path('postgres', None),
                                    sql.rstrip(),
                                ),
                            )
                        
                        for settings_cluster_descr in settings_cluster_descr_list:
                            for i, sql in enumerate(
                                        upgrade_sql.read_upgrade_sql(
                                            settings_cluster_descr,
                                            host_type,
                                            final_migr,
                                        ),
                                    ):
                                if not i:
                                    verb.execute_sql(host_name, 'settings_upgrade_sql')
                                
                                recv.execute(
                                    host_name, '{}\n\n{}\n\n;'.format(
                                        pg_role_path.pg_role_path('postgres', None),
                                        sql.rstrip(),
                                    ),
                                )
                
                for host in hosts_descr.host_list:
                    host_name = host['name']
                    host_type = host['type']
                    
                    for schema_name, owner, grant_list, sql_iter in \
                            install_sql.read_func_install_sql(source_code_cluster_descr, host_type):
                        recv.execute(host_name, pg_role_path.pg_role_path('postgres', None))
                        
                        verb.create_schema(host_name, schema_name)
                        
                        recv.execute(
                            host_name, 
                            install_sql.create_schema(schema_name, owner, grant_list),
                        )
                        
                        for i, sql in enumerate(sql_iter):
                            if not i:
                                verb.execute_sql(host_name, 'func_install_sql')
                            
                            recv.execute(
                                host_name, '{}\n\n{}\n\n;'.format(
                                    pg_role_path.pg_role_path(owner, schema_name),
                                    sql.rstrip(),
                                ),
                            )
            
            for host in hosts_descr.host_list:
                host_name = host['name']
                host_type = host['type']
                
                for i, sql in enumerate(
                            safeguard_sql.read_safeguard_sql(source_code_cluster_descr, host_type),
                        ):
                    if not i:
                        verb.execute_sql(host_name, 'safeguard_sql')
                    
                    recv.execute(
                        host_name, '{}\n\n{}\n\n;'.format(
                            pg_role_path.pg_role_path('postgres', None),
                            sql.rstrip(),
                        ),
                    )
            
            for host in hosts_descr.host_list:
                host_name = host['name']
                host_type = host['type']
                
                var_schemas = install.var_schemas(source_code_cluster_descr, host_type)
                func_schemas = install.func_schemas(source_code_cluster_descr, host_type)
                
                recv.execute(host_name, pg_role_path.pg_role_path('postgres', None))
                
                for schema_name, owner, grant_list, sql_iter in \
                        install_sql.read_var_install_sql(source_code_cluster_descr, host_type):
                    verb.guard_acls(host_name, schema_name)
                    
                    recv.execute(
                        host_name,
                        install_sql.guard_acls(schema_name, owner, grant_list),
                    )
                
                for schema_name, owner, grant_list, sql_iter in \
                        install_sql.read_func_install_sql(source_code_cluster_descr, host_type):
                    verb.guard_acls(host_name, schema_name)
                    
                    recv.execute(
                        host_name,
                        install_sql.guard_acls(schema_name, owner, grant_list),
                    )
                
                verb.push_var_revision(host_name, source_code_cluster_descr.revision, com)
                
                recv.execute(
                    host_name,
                    rev_sql.push_var_revision(source_code_cluster_descr.revision, com, var_schemas),
                )
                
                verb.push_func_revision(host_name, source_code_cluster_descr.revision, com)
                
                recv.execute(
                    host_name,
                    rev_sql.push_func_revision(source_code_cluster_descr.revision, com, func_schemas),
                )
                
                verb.clean_scr_env(host_name)
                
                recv.execute(host_name, scr_env.clean_scr_env())
        
        recv.finish(hosts_descr, finish_host_verb_func=verb.finish_host)
