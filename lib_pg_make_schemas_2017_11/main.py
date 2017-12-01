# -*- mode: python; coding: utf-8 -*-

import sys
import argparse

class ArgsCtx:
    pass

def inspect_cmd(args_ctx, print_func, err_print_func):
    raise NotImplementedError('inspect_cmd is not implemented yet')

def install_cmd(args_ctx, print_func, err_print_func):
    raise NotImplementedError('install_cmd is not implemented yet')

def upgrade_cmd(args_ctx, print_func, err_print_func):
    raise NotImplementedError('upgrade_cmd is not implemented yet')

def install_settings_cmd(args_ctx, print_func, err_print_func):
    raise NotImplementedError('install_settings_cmd is not implemented yet')

def try_print(*args, **kwargs):
    try:
        print(*args, **kwargs)
    except OSError:
        pass

def try_err_print(*args, **kwargs):
    kwargs.setdefault('file', sys.stderr)
    
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
    
    inspect_parser = subparsers.add_parser(
        'inspect',
        help='do inspection of source code for some rude errors',
        description='do inspection of source code for some rude errors',
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
    
    for sub_parser in (install_parser, upgrade_parser, install_settings_parser):
        sub_parser.add_argument(
            '-o',
            '--output',
            help='prefix to output SQL files. this makes output SQL files '
                    'instead of doing database interactions. '
                    'warning(!) the result may be different from the result of database interactions. '
                    'this conduct is less smart and more dangerous',
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
        '-r',
        '--rev',
        help='do upgrading from this revision only',
    )
    
    for sub_parser in (install_parser, upgrade_parser, install_settings_parser):
        sub_parser.add_argument(
            'hosts',
            help='path to the hosts file. if \'-\' is used, it is '
                    'considered as the empty hosts file. that may be useful '
                    'when the ``--output`` option is used',
        )
    
    for sub_parser in (inspect_parser, install_parser, upgrade_parser):
        if sub_parser == inspect_parser:
            arg_help='path to source code for inspection'
        elif sub_parser == upgrade_parser:
            arg_help='path to source code. will be used migration files'
        else:
            arg_help='path to source code. won\'t be used migration files'
        
        sub_parser.add_argument(
            'source_code',
            help=arg_help,
        )
        
        del arg_help
    
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
    
    if args_ctx.command in ('install', 'upgrade', 'install-settings'):
        args_ctx.output = args.output
        args_ctx.hosts = args.hosts
        args_ctx.settings_source_code = args.settings_source_code
        
        if args_ctx.hosts == '-':
            args_ctx.hosts = None
    else:
        args_ctx.output = None
        args_ctx.hosts = None
        args_ctx.settings_source_code = None
    
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
        args_ctx.rev = args.rev
    else:
        args_ctx.rev = None
    
    if args_ctx.command in ('inspect', 'install', 'upgrade'):
        args_ctx.source_code = args.source_code
    else:
        args_ctx.source_code = None
    
    cmd_func_map = {
        'inspect': inspect_cmd,
        'install': install_cmd,
        'upgrade': upgrade_cmd,
        'install-settings': install_settings_cmd,
    }
    
    cmd_func = cmd_func_map[args_ctx.command]
    
    cmd_func(args_ctx, try_print, try_err_print)
