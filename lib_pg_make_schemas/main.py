# -*- mode: python; coding: utf-8 -*-

import sys
import argparse

class ArgsCtx:
    pass

def init_cmd(args_ctx, print_func, err_print_func):
    raise NotImplementedError('init_cmd is not implemented yet')

def install_cmd(args_ctx, print_func, err_print_func):
    from . import install_cmd
    
    install_cmd.install_cmd(args_ctx, print_func, err_print_func)

def upgrade_cmd(args_ctx, print_func, err_print_func):
    raise NotImplementedError('upgrade_cmd is not implemented yet')

def install_settings_cmd(args_ctx, print_func, err_print_func):
    raise NotImplementedError('install_settings_cmd is not implemented yet')

def inspect_cmd(args_ctx, print_func, err_print_func):
    raise NotImplementedError('inspect_cmd is not implemented yet')

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
    parser = argparse.ArgumentParser(
        description='an utility for installing and upgrading database schemas '
                'from a revisioned source code repository.',
    )
    
    subparsers = parser.add_subparsers(
        dest='command',
    )
    
    init_parser = subparsers.add_parser(
        'init',
        help='do some basic initialization of schemas, e.g., creation of extensions and roles',
        description='do some basic initialization of schemas, e.g., creation of extensions and roles',
    )
    
    install_parser = subparsers.add_parser(
        'install',
        help='do fresh installing schemas',
        description='do fresh installing schemas',
    )
    
    upgrade_parser = subparsers.add_parser(
        'upgrade',
        help='do upgrading schemas from a previous version',
        description='upgrading schemas from a previous version',
    )
    
    install_settings_parser = subparsers.add_parser(
        'install-settings',
        help='do fresh installing schema settings',
        description='do fresh installing schema settings',
    )
    
    inspect_parser = subparsers.add_parser(
        'inspect',
        help='do inspection of source code for some rude errors',
        description='do inspection of source code for some rude errors',
    )
    
    for sub_parser in (init_parser, install_parser, upgrade_parser, install_settings_parser):
        sub_parser.add_argument(
            '-e',
            '--execute',
            action='store_true',
            help='do database interactions. '
                    'it is default when the ``--output`` option is not used',
        )
        
        sub_parser.add_argument(
            '-o',
            '--output',
            help='prefix to output SQL files. this makes output SQL files '
                    'instead of doing database interactions (besides '
                    'doing database interactions when the ``--execute`` option is used). '
                    'warning(!) the result may be different from the result of database interactions. '
                    'the output code is less smart and it can be more dangerous',
        )
    
    for sub_parser in (init_parser, install_parser, upgrade_parser, install_settings_parser, inspect_parser):
        sub_parser.add_argument(
            '-i',
            '--include',
            action='append',
            help='add this path to allowed list of directories which can be '
                    'refered from source code files or settings source code files. '
                    'you can use this option many times',
        )
    
    install_parser.add_argument(
        '--reinstall',
        action='store_true',
        help='do dropping schemas before creating new schemas '
                'including variable schemas. '
                'warning(!) your data will be deleted',
    )
    
    install_parser.add_argument(
        '--reinstall-funcs',
        action='store_true',
        help='it is like ``--reinstall`` option, but doesn\'t touch variable schemas. '
                'so your data will be safe, but variable schemas '
                'might become incompatible with created function schemas',
    )
    
    upgrade_parser.add_argument(
        '--show-rev-only',
        action='store_true',
        help='do nothing except showing revision information. '
                'you can use ``--rev`` option for checking ability '
                'upgrading from a specific revision',
    )
    
    upgrade_parser.add_argument(
        '--change-rev-only',
        action='store_true',
        help='do nothing except changing revision information. '
                'warning(!) it is dangerous feature, you can mistakenly lose real revision information '
                'when the ``--rev`` option is not used',
    )
    
    upgrade_parser.add_argument(
        '-r',
        '--rev',
        help='do upgrading from this specific revision only. '
                'that may be useful when the hosts file is empty '
                'or when ``--show-rev-only``/``--change-rev-only`` option is used',
    )
    
    for sub_parser in (init_parser, install_parser, upgrade_parser, install_settings_parser):
        sub_parser.add_argument(
            'hosts',
            help='path to the hosts file. if \'-\' is used, it is '
                    'considered as an empty hosts file. that may be useful '
                    'when the ``--output`` option is used',
        )
    
    for sub_parser in (init_parser, install_parser, upgrade_parser, inspect_parser):
        arg_help_map = {
            init_parser: 'path to source code. will be used init files only',
            install_parser: 'path to source code. won\'t be used init and migration files',
            upgrade_parser: 'path to source code. will be used migration files',
            inspect_parser: 'path to source code for inspection',
        }
        
        arg_help = arg_help_map[sub_parser]
        
        sub_parser.add_argument(
            'source_code',
            help=arg_help,
        )
        
        del arg_help
        del arg_help_map
    
    for sub_parser in (install_parser, upgrade_parser, install_settings_parser):
        if sub_parser in (install_parser, upgrade_parser):
            arg_nargs='?'
        else:
            arg_nargs=None
        
        if sub_parser == upgrade_parser:
            arg_help='path to settings source code. will be used migration files'
        else:
            arg_help='path to settings source code. won\'t be used migration files'
        
        sub_parser.add_argument(
            'settings_source_code',
            nargs=arg_nargs,
            help=arg_help,
        )
        
        del arg_nargs
        del arg_help
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        
        return
    
    args_ctx = ArgsCtx()
    
    args_ctx.command = args.command
    
    if args_ctx.command in ('init', 'install', 'upgrade', 'install-settings'):
        args_ctx.execute = args.execute
        args_ctx.output = args.output
        args_ctx.hosts = args.hosts
        
        if args_ctx.output is None:
            args_ctx.execute = True
        
        if args_ctx.hosts == '-':
            args_ctx.hosts = None
    else:
        args_ctx.execute = False
        args_ctx.output = None
        args_ctx.hosts = None
    
    if args_ctx.command in ('init', 'install', 'upgrade', 'install-settings', 'inspect') \
            and args.include is not None:
        args_ctx.include_list = args.include
    else:
        args_ctx.include_list = []
    
    if args_ctx.command == 'install' and args.reinstall:
        args_ctx.reinstall = True
    else:
        args_ctx.reinstall = False
    
    if args_ctx.command == 'install' and args.reinstall_funcs:
        args_ctx.reinstall = True
        args_ctx.reinstall_funcs = True
    else:
        args_ctx.reinstall_funcs = False
    
    if args_ctx.command == 'upgrade':
        if args.show_rev_only:
            args_ctx.show_rev_only = True
        else:
            args_ctx.show_rev_only = False
        
        if args.change_rev_only:
            args_ctx.change_rev_only = True
        else:
            args_ctx.change_rev_only = False
        
        args_ctx.rev = args.rev
    else:
        args_ctx.show_rev_only = False
        args_ctx.change_rev_only = False
        args_ctx.rev = None
    
    if args_ctx.command in ('init', 'install', 'upgrade', 'inspect'):
        args_ctx.source_code = args.source_code
    else:
        args_ctx.source_code = None
    
    if args_ctx.command in ('install', 'upgrade', 'install-settings'):
        args_ctx.settings_source_code = args.settings_source_code
    else:
        args_ctx.settings_source_code = None
    
    cmd_func_map = {
        'init': init_cmd,
        'install': install_cmd,
        'upgrade': upgrade_cmd,
        'install-settings': install_settings_cmd,
        'inspect': inspect_cmd,
    }
    
    cmd_func = cmd_func_map[args_ctx.command]
    
    cmd_func(args_ctx, try_print, try_err_print)
