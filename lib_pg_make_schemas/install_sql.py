from . import pg_literal

GUARD_ACLS_SQL = '''\
declare
_create_list text[] := {q_create_list}::text[];
_usage_list text[] := {q_usage_list}::text[];
_grantor text;
_grantee text;
_privilege_type text;
_is_grantable boolean;
begin
perform 1 from pg_namespace ns
where ns.nspname = {q_schema} and ns.nspacl is null;
if found then
execute format ($revoke$revoke all on schema %I from public$revoke$, {q_schema});
end if;
for _grantor, _grantee, _privilege_type, _is_grantable in
select case when acl.grantor = 0 then 'public'
else (select r.rolname from pg_roles r where oid = acl.grantor) end grantor,
case when acl.grantee = 0 then 'public'
else (select r.rolname from pg_roles r where oid = acl.grantee) end grantee,
acl.privilege_type,
acl.is_grantable
from (
select (aclexplode (ns.nspacl)).*
from pg_namespace ns
where ns.nspname = {q_schema}
) acl
loop
if _grantor = {q_owner} and _grantee = any ({q_create_list}::text[])
and _privilege_type = 'CREATE' and _is_grantable = false then
_create_list := array_remove (_create_list, _grantee);
elsif _grantor = {q_owner} and _grantee = any ({q_usage_list}::text[])
and _privilege_type = 'USAGE' and _is_grantable = false then
_usage_list := array_remove (_usage_list, _grantee);
else
raise 'unexpected acl: % % % % %',
quote_nullable ({q_schema}), quote_nullable (_grantor), quote_nullable (_grantee),
quote_nullable (_privilege_type), quote_nullable (_is_grantable);
end if;
end loop;
if not array_length (_create_list, 1) is null then
raise 'missing create acls: % %', quote_nullable ({q_schema}), quote_nullable (_create_list);
end if;
if not array_length (_usage_list, 1) is null then
raise 'missing usage acls: % %', quote_nullable ({q_schema}), quote_nullable (_usage_list);
end if;
end\
'''

def read_var_install_sql(cluster_descr, host_type):
    for schemas_descr in cluster_descr.schemas_list:
        if schemas_descr.schemas_type != host_type:
            continue

        for schema_descr in schemas_descr.var_schema_list:
            schema_name = schema_descr.schema_name
            owner = schema_descr.owner
            grant_list = schema_descr.grant_list
            sql_iter = schema_descr.read_sql()

            yield schema_name, owner, grant_list, sql_iter

def read_late_sql(cluster_descr, host_type):
    for schemas_descr in cluster_descr.schemas_list:
        if schemas_descr.schemas_type != host_type:
            continue

        late_descr = schemas_descr.late

        if late_descr is None:
            continue

        yield from late_descr.read_sql()

def read_func_install_sql(cluster_descr, host_type):
    for schemas_descr in cluster_descr.schemas_list:
        if schemas_descr.schemas_type != host_type:
            continue

        for schema_descr in schemas_descr.func_schema_list:
            schema_name = schema_descr.schema_name
            owner = schema_descr.owner
            grant_list = schema_descr.grant_list
            sql_iter = schema_descr.read_sql()

            yield schema_name, owner, grant_list, sql_iter

def create_schema(
            schema_name,
            owner,
            grant_list,
            pg_ident_quote_func=pg_literal.pg_ident_quote,
        ):
    sql_list = [
        'create schema {q_schema_ident};'.format(
            q_schema_ident=pg_ident_quote_func(schema_name),
        ),
        'alter schema {q_schema_ident} owner to {q_owner};'.format(
            q_schema_ident=pg_ident_quote_func(schema_name),
            q_owner=pg_ident_quote_func(owner),
        ),
        'revoke all on schema {q_schema_ident} from public;'.format(
            q_schema_ident=pg_ident_quote_func(schema_name),
        ),
    ]

    if grant_list is not None:
        for grant in grant_list:
            sql_list.append(
                'grant usage on schema {q_schema_ident} to {q_grant};'.format(
                    q_schema_ident=pg_ident_quote_func(schema_name),
                    q_grant=pg_ident_quote_func(grant),
                ),
            )

    return '\n'.join(sql_list)

def guard_acls(
            schema_name,
            owner,
            grant_list,
            guard_acls_sql=GUARD_ACLS_SQL,
            pg_quote_func=pg_literal.pg_quote,
            pg_dollar_quote_func=pg_literal.pg_dollar_quote,
        ):
    create_list = [owner]

    if grant_list is not None:
        usage_list = [owner] + grant_list
    else:
        usage_list = [owner]

    q_create_list = 'array[{}\n]'.format(
        ','.join('\n{}'.format(pg_quote_func(x)) for x in create_list),
    )
    q_usage_list = 'array[{}\n]'.format(
        ','.join('\n{}'.format(pg_quote_func(x)) for x in usage_list),
    )

    drop_schemas_body = guard_acls_sql.format(
        q_schema=pg_quote_func(schema_name),
        q_owner=pg_quote_func(owner),
        q_create_list=q_create_list,
        q_usage_list=q_usage_list,
    )

    return 'do {};'.format(pg_dollar_quote_func('do', drop_schemas_body))

# vi:ts=4:sw=4:et
