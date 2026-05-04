import sys
import os
import argparse

class ArgsCtx:
    pass

def add_shared_options(sub_parser):
    sub_parser.add_argument(
        '-v',
        '--verbose',
        action='count',
        default=0,
        help='show command phases. repeat as -vv to show SQL file '
                'execution details',
    )

    sub_parser.add_argument(
        '-e',
        '--execute',
        action='store_true',
        help='connect to databases and run SQL. this is the default when '
                '--output is not used. with --output, this also writes '
                'PostgreSQL notice files next to SQL files',
    )

    sub_parser.add_argument(
        '-p',
        '--pretend',
        action='store_true',
        help='execute SQL and roll back database transactions instead of '
                'committing them. this implies --execute',
    )

    sub_parser.add_argument(
        '-o',
        '--output',
        metavar='OUTPUT',
        help='write generated SQL files using OUTPUT as the file prefix. '
                'without --execute, only generate files and do not connect '
                'to databases. with --execute, also keep SQL and notice '
                'files as an execution record',
    )

    sub_parser.add_argument(
        '-i',
        '--include',
        metavar='INCLUDE',
        action='append',
        help='allow SQL includes from another directory. use name=value to '
                'define a reusable include reference; repeat as needed',
    )

    sub_parser.add_argument(
        '--host-name',
        metavar='HOST_NAME',
        help='use only the resolved host whose name is HOST_NAME. this can '
                'make a single-host run from a larger hosts list',
    )

    sub_parser.add_argument(
        '-C',
        '--conninfo',
        metavar='CONNINFO',
        help='use CONNINFO for the target host. requires a single-host run',
    )

    sub_parser.add_argument(
        '-D',
        '--define',
        metavar='NAME=VALUE',
        action='append',
        help='set NAME=VALUE in the shared script environment. '
                'repeat as needed',
    )

    sub_parser.add_argument(
        '-d',
        '--host-define',
        metavar='NAME=VALUE',
        action='append',
        help='set NAME=VALUE in the selected host params mapping. repeat as '
                'needed. requires a single-host run',
    )

    sub_parser.add_argument(
        '-X',
        '--exclusive',
        action='store_true',
        help='abort if a *_revision schema for another application already '
                'exists in the database. requires a single-host run',
    )

def add_install_upgrade_options(sub_parser):
    sub_parser.add_argument(
        '-c',
        '--comment',
        action='store_true',
        help='run comment.sh to record source/deployment provenance with '
                'revision metadata. use this only with trusted scripts. '
                'PG_MAKE_SCHEMAS_COMMENT sets the script path and implies '
                'this option',
    )

    sub_parser.add_argument(
        '--init',
        action='store_true',
        help='run basic initialization before install or upgrade. the '
                'standalone init command may be safer for initialization '
                'that touches shared database-cluster state',
    )

def add_drop_acl_options(sub_parser):
    sub_parser.add_argument(
        '--cascade',
        action='store_true',
        help='use DROP SCHEMA ... CASCADE when dropping schemas. this can be '
                'dangerous; review the possible consequences first',
    )

    sub_parser.add_argument(
        '-A',
        '--weak-acls',
        action='store_true',
        help='turn unexpected ACL errors into notices during ACL guarding',
    )

def add_install_options(install_parser):
    install_parser.add_argument(
        '--reinstall',
        action='store_true',
        help='drop variable and function schemas before installing. this '
                'deletes data and requires --cascade',
    )

    install_parser.add_argument(
        '--reinstall-func',
        action='store_true',
        help='drop and recreate only function schemas. variable schemas and '
                'their data are left in place',
    )

def add_upgrade_options(upgrade_parser):
    upgrade_parser.add_argument(
        '--show-rev',
        action='store_true',
        help='show stored revision information and stop. use with --rev to '
                'check whether an upgrade path exists from a specific '
                'revision',
    )

    upgrade_parser.add_argument(
        '--change-rev',
        action='store_true',
        help='change stored revision information and stop. this is dangerous; '
                'without --rev, it can overwrite real revision information',
    )

    upgrade_parser.add_argument(
        '-r',
        '--rev',
        metavar='REV',
        help='use REV as the starting revision for offline generation, '
                '--show-rev checks, or --change-rev instead of reading the '
                'database',
    )

    upgrade_parser.add_argument(
        '--install',
        action='store_true',
        help='fall back to install for hosts that do not have a stored '
                'variable revision',
    )

def add_shared_positional_args(init_parser, install_parser, upgrade_parser):
    for sub_parser in (init_parser, install_parser, upgrade_parser):
        sub_parser.add_argument(
            'hosts',
            metavar='HOSTS',
            help='path to the hosts file. use - to build pseudo-hosts from '
                    'source tree schema types',
        )

        source_code_help_map = {
            init_parser: 'path to the source tree. only init files are used',
            install_parser: 'path to the source tree. migration files are '
                    'not used',
            upgrade_parser: 'path to the source tree. migration files are used',
        }

        sub_parser.add_argument(
            'source_code',
            metavar='SOURCE_CODE',
            help=source_code_help_map[sub_parser],
        )

    for sub_parser in (install_parser, upgrade_parser):
        settings_source_code_help_map = {
            install_parser: 'path to a settings source tree. migration files '
                    'are not used',
            upgrade_parser: 'path to a settings source tree. migration files '
                    'are used',
        }

        sub_parser.add_argument(
            'settings_source_code',
            metavar='SETTINGS_SOURCE_CODE',
            nargs='*',
            help=settings_source_code_help_map[sub_parser],
        )

def make_parser():
    parser = argparse.ArgumentParser(
        description='a utility for installing and upgrading database schemas '
                'from a versioned source-code repository.',
    )

    subparsers = parser.add_subparsers(
        dest='command',
    )

    init_parser = subparsers.add_parser(
        'init',
        help='run basic schema initialization, such as idempotent creation '
                'of extensions and roles',
        description='run basic schema initialization, such as idempotent '
                'creation of extensions and roles',
    )

    install_parser = subparsers.add_parser(
        'install',
        help='install schemas into a fresh database',
        description='install schemas into a fresh database',
    )

    upgrade_parser = subparsers.add_parser(
        'upgrade',
        help='upgrade schemas from a previous revision',
        description='upgrade schemas from a previous revision',
    )

    for sub_parser in (init_parser, install_parser, upgrade_parser):
        add_shared_options(sub_parser)

    for sub_parser in (install_parser, upgrade_parser):
        add_install_upgrade_options(sub_parser)

    add_install_options(install_parser)

    for sub_parser in (install_parser, upgrade_parser):
        add_drop_acl_options(sub_parser)

    add_upgrade_options(upgrade_parser)
    add_shared_positional_args(init_parser, install_parser, upgrade_parser)

    return parser

def parse_define_map(arg_define_list):
    define_map = {}

    if arg_define_list is not None:
        for arg_define in arg_define_list:
            if '=' not in arg_define:
                raise ValueError('unable to parse define without =')

            arg_define_name, arg_define_val = arg_define.split('=', 1)

            define_map[arg_define_name] = arg_define_val

    return define_map

def make_args_ctx(args):
    args_ctx = ArgsCtx()

    args_ctx.command = args.command
    args_ctx.verbose = args.verbose
    args_ctx.execute = args.execute
    args_ctx.pretend = args.pretend
    args_ctx.output = args.output
    args_ctx.hosts = args.hosts
    args_ctx.host_name = args.host_name
    args_ctx.conninfo = args.conninfo
    args_ctx.define_map = parse_define_map(args.define)
    args_ctx.host_define_map = parse_define_map(args.host_define)

    if args_ctx.pretend or args_ctx.output is None:
        args_ctx.execute = True

    if args_ctx.hosts == '-':
        args_ctx.hosts = None

    args_ctx.exclusive = args.exclusive

    args_ctx.include_list = []
    args_ctx.include_ref_map = {}

    if args.include is not None:
        for arg_inc in args.include:
            if '=' in arg_inc:
                arg_inc_ref_name, arg_inc_ref_val = arg_inc.split('=', 1)

                args_ctx.include_list.append(arg_inc_ref_val)
                args_ctx.include_ref_map[arg_inc_ref_name] = arg_inc_ref_val
            else:
                args_ctx.include_list.append(arg_inc)

    args_ctx.reinstall = False
    args_ctx.reinstall_func = False

    if args_ctx.command == 'install':
        args_ctx.reinstall = args.reinstall or args.reinstall_func
        args_ctx.reinstall_func = args.reinstall_func

    args_ctx.install = args_ctx.command == 'upgrade' and args.install

    if args_ctx.command in ('install', 'upgrade'):
        args_ctx.comment = args.comment
        args_ctx.init = args.init
        args_ctx.cascade = args.cascade
        args_ctx.weak_guard_acls = args.weak_acls

        args_ctx.comment_path = os.environ.get('PG_MAKE_SCHEMAS_COMMENT')

        if args_ctx.comment_path is not None:
            args_ctx.comment = True
    else:
        args_ctx.comment = False
        args_ctx.init = False
        args_ctx.cascade = None
        args_ctx.weak_guard_acls = None
        args_ctx.comment_path = None

    if args_ctx.command == 'upgrade':
        args_ctx.show_rev = args.show_rev
        args_ctx.change_rev = args.change_rev
        args_ctx.rev = args.rev
    else:
        args_ctx.show_rev = False
        args_ctx.change_rev = False
        args_ctx.rev = None

    args_ctx.source_code = args.source_code

    if args_ctx.command in ('install', 'upgrade'):
        args_ctx.settings_source_code = args.settings_source_code
    else:
        args_ctx.settings_source_code = []

    return args_ctx

def init_cmd(args_ctx, print_func, err_print_func):
    from . import init_cmd

    init_cmd.init_cmd(args_ctx, print_func, err_print_func)

def install_cmd(args_ctx, print_func, err_print_func):
    from . import install_cmd

    install_cmd.install_cmd(args_ctx, print_func, err_print_func)

def upgrade_cmd(args_ctx, print_func, err_print_func):
    from . import upgrade_cmd

    upgrade_cmd.upgrade_cmd(args_ctx, print_func, err_print_func)

def try_print(*args, **kwargs):
    kwargs.setdefault('flush', True)

    try:
        print(*args, **kwargs)
    except OSError:
        pass

def try_err_print(*args, **kwargs):
    kwargs.setdefault('file', sys.stderr)
    kwargs.setdefault('flush', True)

    try:
        print(*args, **kwargs)
    except OSError:
        pass

def main():
    parser = make_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()

        return

    args_ctx = make_args_ctx(args)
    cmd_func_map = {
        'init': init_cmd,
        'install': install_cmd,
        'upgrade': upgrade_cmd,
    }

    cmd_func = cmd_func_map[args_ctx.command]

    cmd_func(args_ctx, try_print, try_err_print)

# vi:ts=4:sw=4:et
