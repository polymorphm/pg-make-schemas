import json
from . import pg_literal

def _json_dumps(value):
    return json.dumps(value, indent=4)

def scr_env(
            hosts_descr,
            host_name,
            json_dumps_func=_json_dumps,
            pg_quote_func=pg_literal.pg_quote,
            pg_dollar_quote_func=pg_literal.pg_dollar_quote,
        ):
    host_type = None
    host_params = None

    for other_host in hosts_descr.host_list:
        other_host_name = other_host['name']
        other_host_type = other_host['type']
        other_host_params = other_host['params']

        if other_host_name == host_name:
            host_type = other_host_type
            host_params = other_host_params

    host_name_body = 'select {}::text'.format(pg_quote_func(host_name))
    host_type_body = 'select {}::text'.format(pg_quote_func(host_type))
    host_params_body = 'select {}::json'.format(
        pg_dollar_quote_func('json', json_dumps_func(host_params)),
    )
    shared_body = 'select {}::json'.format(
        pg_dollar_quote_func('json', json_dumps_func(hosts_descr.shared)),
    )

    func_list = [
        'create function pg_temp.scr_env_host_name ()\n'
                'returns text language sql stable\n'
                'as {};'.format(
                    pg_dollar_quote_func('function', host_name_body),
                ),
        'create function pg_temp.scr_env_host_type ()\n'
                'returns text language sql stable\n'
                'as {};'.format(
                    pg_dollar_quote_func('function', host_type_body),
                ),
        'create function pg_temp.scr_env_host_params ()\n'
                'returns json language sql stable\n'
                'as {};'.format(
                    pg_dollar_quote_func('function', host_params_body),
                ),
        'create function pg_temp.scr_env_shared ()\n'
                'returns json language sql stable\n'
                'as {};'.format(
                    pg_dollar_quote_func('function', shared_body),
                ),
    ]

    return '\n\n'.join(func_list)

def clean_scr_env():
    func_list = [
        'drop function pg_temp.scr_env_host_name ();',
        'drop function pg_temp.scr_env_host_type ();',
        'drop function pg_temp.scr_env_host_params ();',
        'drop function pg_temp.scr_env_shared ();',
    ]

    return '\n'.join(func_list)

# vi:ts=4:sw=4:et
