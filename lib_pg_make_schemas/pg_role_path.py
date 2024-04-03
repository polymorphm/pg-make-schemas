from . import pg_literal

def pg_role_path(
            role,
            schema_name,
            pg_ident_quote_func=pg_literal.pg_ident_quote,
        ):
    if role is not None:
        set_list = [
            'set local role to {};'.format(
                pg_ident_quote_func(role),
            ),
        ]
    else:
        set_list = ['set local role to postgres;']

    if schema_name is not None:
        set_list.append(
            'set local search_path to {};'.format(
                pg_ident_quote_func(schema_name),
            ),
        )
    else:
        set_list.append('set local search_path to \'\';')

    set_list.append('set local check_function_bodies to off;')

    return '\n'.join(set_list)

def apply_pg_role_path(
            sql,
            role,
            schema_name,
            pg_role_path_func=pg_role_path,
            pg_ident_quote_func=pg_literal.pg_ident_quote,
        ):
    if isinstance(sql, tuple):
        sql_list_or_str, sql_info = sql
        if isinstance(sql_list_or_str, list):
            sql_str_list = sql_list_or_str
        elif isinstance(sql_list_or_str, str):
            sql_str_list = [sql_list_or_str]
        else:
            raise TypeError
    elif isinstance(sql, str):
        sql_str, sql_info = sql, {}
        sql_str_list = [sql_str]
    else:
        raise TypeError

    if not sql_str_list:
        return sql_str_list, sql_info

    new_sql_str_list = [
        '{}\n\n'.format(
            pg_role_path_func(role, schema_name, pg_ident_quote_func=pg_ident_quote_func),
        )
    ] + sql_str_list[:-1] + ['{}\n\n;'.format(sql_str_list[-1].rstrip())]

    new_sql_info = sql_info.copy()
    new_sql_info.update({
        'pg_role': role,
        'pg_search_path': schema_name,
    })

    return new_sql_str_list, new_sql_info

# vi:ts=4:sw=4:et
