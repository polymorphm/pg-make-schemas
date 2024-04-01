import os, os.path
import contextlib
from . import verbose
from . import descr
from . import revision_sql
from . import comment
from . import receivers
from . import install
from . import settings
from . import pg_role_path
from . import scr_env
from . import init_sql
from . import install_sql
from . import settings_sql
from . import safeguard_sql

class InstallCmdError(Exception):
    pass

def install_cmd(args_ctx, print_func, err_print_func):
    if args_ctx.reinstall and not args_ctx.reinstall_func and not args_ctx.cascade:
        raise InstallCmdError('unable to reinstall variable schemas without cascaded dropping')

    verb = verbose.make_verbose(print_func, err_print_func, args_ctx.verbose)

    verb.prepare_install()

    hosts_descr = descr.HostsDescr()

    if args_ctx.hosts is not None:
        hosts_path = os.path.realpath(args_ctx.hosts)

        hosts_descr.load(hosts_path)

    include_list = []
    include_ref_map = {}

    for include in args_ctx.include_list:
        include_list.append(os.path.realpath(include))

    for include_ref_name in args_ctx.include_ref_map:
        include_ref_map[include_ref_name] = \
                os.path.realpath(args_ctx.include_ref_map[include_ref_name])

    source_code_file_path = os.path.realpath(os.path.join(
        args_ctx.source_code,
        descr.ClusterDescr.file_name,
    ))
    source_code_include_list = include_list + [os.path.dirname(source_code_file_path)]
    source_code_cluster_descr = descr.ClusterDescr()

    source_code_cluster_descr.load(
            source_code_file_path, source_code_include_list, include_ref_map)

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
            include_ref_map,
            settings_mode=True,
        )

        settings.check_settings_compatibility(
            source_code_cluster_descr,
            settings_cluster_descr,
        )

        settings_cluster_descr_list.append(settings_cluster_descr)

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

            var_schemas = install.var_schemas(source_code_cluster_descr, host_type)
            func_schemas = install.func_schemas(source_code_cluster_descr, host_type)

            recv.execute(host_name, pg_role_path.pg_role_path(None, None))

            verb.scr_env(host_name, recv.look_fragment_i(host_name))

            recv.execute(host_name, scr_env.scr_env(hosts_descr, host_name))

            verb.ensure_revision_structs(host_name, recv.look_fragment_i(host_name))

            recv.execute(host_name, rev_sql.ensure_revision_structs(host_type))

            if args_ctx.reinstall:
                if not args_ctx.reinstall_func:
                    verb.drop_var_schemas(host_name, args_ctx.cascade, recv.look_fragment_i(host_name))

                    recv.execute(host_name, rev_sql.drop_var_schemas(host_type, var_schemas, args_ctx.cascade))

                verb.drop_func_schemas(host_name, args_ctx.cascade, recv.look_fragment_i(host_name))

                recv.execute(host_name, rev_sql.drop_func_schemas(host_type, func_schemas, args_ctx.cascade))

                if not args_ctx.reinstall_func:
                    verb.clean_var_revision(host_name, recv.look_fragment_i(host_name))

                    recv.execute(host_name, rev_sql.clean_var_revision(host_type))

                verb.clean_func_revision(host_name, recv.look_fragment_i(host_name))

                recv.execute(host_name, rev_sql.clean_func_revision(host_type))

            if not args_ctx.reinstall_func:
                verb.guard_var_revision(host_name, None, recv.look_fragment_i(host_name))

                recv.execute(host_name, rev_sql.guard_var_revision(host_type, None))

            verb.guard_func_revision(host_name, None, recv.look_fragment_i(host_name))

            recv.execute(host_name, rev_sql.guard_func_revision(host_type, None))

        if args_ctx.init:
            for host in hosts_descr.host_list:
                host_name = host['name']
                host_type = host['type']

                for i, sql in enumerate(
                            init_sql.read_init_sql(source_code_cluster_descr, host_type),
                        ):
                    if not i:
                        verb.execute_sql(host_name, 'init_sql', recv.look_fragment_i(host_name))

                    sql = pg_role_path.apply_pg_role_path(sql, None, None)

                    verb.execute_sql(host_name, 'init_sql', recv.look_fragment_i(host_name),
                            sql=sql)

                    recv.execute(host_name, sql)

        if not args_ctx.reinstall_func:
            for host in hosts_descr.host_list:
                host_name = host['name']
                host_type = host['type']

                for schema_name, owner, grant_list, sql_iter in \
                        install_sql.read_var_install_sql(source_code_cluster_descr, host_type):
                    recv.execute(host_name, pg_role_path.pg_role_path(None, None))

                    verb.create_schema(host_name, schema_name, recv.look_fragment_i(host_name))

                    recv.execute(
                        host_name,
                        install_sql.create_schema(schema_name, owner, grant_list),
                    )

                    for i, sql in enumerate(sql_iter):
                        if not i:
                            verb.execute_sql(
                                    host_name, 'var_install_sql', recv.look_fragment_i(host_name))

                        sql = pg_role_path.apply_pg_role_path(sql, owner, schema_name)

                        verb.execute_sql(
                                host_name, 'var_install_sql', recv.look_fragment_i(host_name),
                                sql=sql)

                        recv.execute(host_name, sql)

                for i, sql in enumerate(
                            install_sql.read_late_sql(source_code_cluster_descr, host_type),
                        ):
                    if not i:
                        verb.execute_sql(
                                host_name, 'late_install_sql', recv.look_fragment_i(host_name))

                    sql = pg_role_path.apply_pg_role_path(sql, None, None)

                    verb.execute_sql(
                            host_name, 'late_install_sql', recv.look_fragment_i(host_name),
                            sql=sql)

                    recv.execute(host_name, sql)

        for settings_cluster_descr in settings_cluster_descr_list:
            for host in hosts_descr.host_list:
                host_name = host['name']
                host_type = host['type']

                for i, sql in enumerate(
                            settings_sql.read_settings_sql(settings_cluster_descr, host_type),
                        ):
                    if not i:
                        verb.execute_sql(
                                host_name, 'settings_sql', recv.look_fragment_i(host_name))

                    sql = pg_role_path.apply_pg_role_path(sql, None, None)

                    verb.execute_sql(
                            host_name, 'settings_sql', recv.look_fragment_i(host_name),
                            sql=sql)

                    recv.execute(host_name, sql)

        for host in hosts_descr.host_list:
            host_name = host['name']
            host_type = host['type']

            for schema_name, owner, grant_list, sql_iter in \
                    install_sql.read_func_install_sql(source_code_cluster_descr, host_type):
                recv.execute(host_name, pg_role_path.pg_role_path(None, None))

                verb.create_schema(host_name, schema_name, recv.look_fragment_i(host_name))

                recv.execute(
                    host_name,
                    install_sql.create_schema(schema_name, owner, grant_list),
                )

                for i, sql in enumerate(sql_iter):
                    if not i:
                        verb.execute_sql(
                                host_name, 'func_install_sql', recv.look_fragment_i(host_name))

                    sql = pg_role_path.apply_pg_role_path(sql, owner, schema_name)

                    verb.execute_sql(
                            host_name, 'func_install_sql', recv.look_fragment_i(host_name),
                            sql=sql)

                    recv.execute(host_name, sql)

        for host in hosts_descr.host_list:
            host_name = host['name']
            host_type = host['type']

            for i, sql in enumerate(
                        safeguard_sql.read_safeguard_sql(source_code_cluster_descr, host_type),
                    ):
                if not i:
                    verb.execute_sql(
                            host_name, 'safeguard_sql', recv.look_fragment_i(host_name))

                sql = pg_role_path.apply_pg_role_path(sql, None, None)

                verb.execute_sql(
                        host_name, 'safeguard_sql', recv.look_fragment_i(host_name),
                        sql=sql)

                recv.execute(host_name, sql)

        for host in hosts_descr.host_list:
            host_name = host['name']
            host_type = host['type']

            var_schemas = install.var_schemas(source_code_cluster_descr, host_type)
            func_schemas = install.func_schemas(source_code_cluster_descr, host_type)

            recv.execute(host_name, pg_role_path.pg_role_path(None, None))

            if not args_ctx.reinstall_func:
                for schema_name, owner, grant_list, sql_iter in \
                        install_sql.read_var_install_sql(source_code_cluster_descr, host_type):
                    verb.guard_acls(host_name, schema_name,
                            args_ctx.weak_guard_acls, recv.look_fragment_i(host_name))

                    recv.execute(
                        host_name,
                        install_sql.guard_acls(schema_name, owner, grant_list,
                                args_ctx.weak_guard_acls),
                    )

            for schema_name, owner, grant_list, sql_iter in \
                    install_sql.read_func_install_sql(source_code_cluster_descr, host_type):
                verb.guard_acls(host_name, schema_name,
                        args_ctx.weak_guard_acls, recv.look_fragment_i(host_name))

                recv.execute(
                    host_name,
                    install_sql.guard_acls(schema_name, owner, grant_list,
                            args_ctx.weak_guard_acls),
                )

            if not args_ctx.reinstall_func:
                verb.push_var_revision(
                        host_name, source_code_cluster_descr.revision, com, recv.look_fragment_i(host_name))

                recv.execute(
                    host_name,
                    rev_sql.push_var_revision(host_type, source_code_cluster_descr.revision, com, var_schemas),
                )

            verb.push_func_revision(
                    host_name, source_code_cluster_descr.revision, com, recv.look_fragment_i(host_name))

            recv.execute(
                host_name,
                rev_sql.push_func_revision(host_type, source_code_cluster_descr.revision, com, func_schemas),
            )

            verb.clean_scr_env(host_name, recv.look_fragment_i(host_name))

            recv.execute(host_name, scr_env.clean_scr_env())

        recv.finish(hosts_descr, finish_host_verb_func=verb.finish_host)

# vi:ts=4:sw=4:et
