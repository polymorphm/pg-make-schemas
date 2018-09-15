# -*- mode: python; coding: utf-8 -*-

from . import pg_literal

CREATE_REVISION_SCHEMA_SQL = '''\
create schema if not exists {q_revision_schema_ident};
revoke all on schema {q_revision_schema_ident} from public;\
'''

CREATE_REVISION_TABLE_SQL = '''\
create table if not exists {q_revision_schema_ident}.{q_revision_ident} (
application text not null,
schemas_type text not null,
primary key (application, schemas_type),
datetime timestamp with time zone not null,
revision text not null,
comment text,
schemas text[]
);\
'''

CREATE_REVISION_HISTORY_TABLE_SQL = '''\
create table if not exists {q_revision_schema_ident}.{q_revision_history_ident} (
revision_history_id bigserial primary key,
application text not null,
schemas_type text not null,
unique (application, schemas_type, revision_history_id),
datetime timestamp with time zone not null,
revision text not null,
comment text,
schemas text[]
);\
'''

FETCH_REVISION_SQL ='''\
select rev.revision, rev.comment
from {q_revision_schema_ident}.{q_revision_ident} rev
where rev.application = %(application)s and rev.schemas_type = %(schemas_type)s
for update\
'''

GUARD_REVISION_SQL ='''\
declare
_revision text;
begin
select rev.revision
into _revision
from {q_revision_schema_ident}.{q_revision_ident} rev
where rev.application = {q_application} and rev.schemas_type = {q_schemas_type}
for update;
if _revision is distinct from {q_revision} then
raise 'unexpected revision: % %', quote_nullable (_revision), quote_nullable ({q_revision});
end if;
end\
'''

CLEAN_REVISION_SQL ='''\
delete from {q_revision_schema_ident}.{q_revision_ident} rev
where rev.application = {q_application} and rev.schemas_type = {q_schemas_type};\
'''

PUSH_REVISION_SQL ='''\
with ins_rev as (
insert into {q_revision_schema_ident}.{q_revision_ident}
(application, schemas_type, datetime, revision, comment, schemas)
values ({q_application}, {q_schemas_type}, now (), {q_revision}, {q_comment}, {q_schemas}::text[])
returning *
)
insert into {q_revision_schema_ident}.{q_revision_history_ident}
(application, schemas_type, datetime, revision, comment, schemas)
select rev.application, rev.schemas_type, rev.datetime, rev.revision, rev.comment, rev.schemas
from ins_rev rev;\
'''

DROP_SCHEMAS_SQL ='''\
declare
_schema text;
begin
for _schema in
select unnest (rev.schemas)
from {q_revision_schema_ident}.{q_revision_ident} rev
where rev.application = {q_application} and rev.schemas_type = {q_schemas_type}
union
select unnest ({q_schemas}::text[])
loop
execute format ($drop$drop schema if exists %I cascade$drop$, _schema);
end loop;
end\
'''

class RevisionSqlError(Exception):
    pass

class RevisionSqlUtils:
    @classmethod
    def make_ident(cls, value):
        return value.replace('-', '_').lower()
    
    @classmethod
    def revision_schema_ident(cls, application_ident):
        return '{}_revision'.format(application_ident)
    
    @classmethod
    def var_revision_ident(cls, host_type_ident):
        return '{}_var_revision'.format(host_type_ident)
    
    @classmethod
    def func_revision_ident(cls, host_type_ident):
        return '{}_func_revision'.format(host_type_ident)
    
    @classmethod
    def var_revision_history_ident(cls, host_type_ident):
        return '{}_var_revision_history'.format(host_type_ident)
    
    @classmethod
    def func_revision_history_ident(cls, host_type_ident):
        return '{}_func_revision_history'.format(host_type_ident)

class RevisionSql:
    _create_revision_schema_sql = CREATE_REVISION_SCHEMA_SQL
    _create_revision_table_sql = CREATE_REVISION_TABLE_SQL
    _create_revision_history_table_sql = CREATE_REVISION_HISTORY_TABLE_SQL
    _fetch_revision_sql = FETCH_REVISION_SQL
    _guard_revision_sql = GUARD_REVISION_SQL
    _clean_revision_sql = CLEAN_REVISION_SQL
    _push_revision_sql = PUSH_REVISION_SQL
    _drop_schemas_sql = DROP_SCHEMAS_SQL
    _revision_sql_utils = RevisionSqlUtils
    
    def __init__(self, application):
        self._application = application
    
    def _pg_quote(self, value):
        return pg_literal.pg_quote(value)
    
    def _pg_ident_quote(self, ident):
        return pg_literal.pg_ident_quote(ident)
    
    def _pg_dollar_quote(self, tag, value):
        return pg_literal.pg_dollar_quote(tag, value)
    
    def _fetch_revision(self, recv, host_name, revision_schema_ident, revision_ident, host_type):
        con = recv.get_con(host_name)
        
        try:
            with con.cursor() as cur:
                cur.execute(
                    self._fetch_revision_sql.format(
                        q_revision_schema_ident=self._pg_ident_quote(
                            revision_schema_ident,
                        ).replace('%', '%%'),
                        q_revision_ident=self._pg_ident_quote(
                            revision_ident,
                        ).replace('%', '%%'),
                    ),
                    {
                        'application': self._application,
                        'schemas_type': host_type,
                    },
                )
                
                row = cur.fetchone()
                
                if row is None:
                    return None, None
                
                revision, comment = row
                
                return revision, comment
        except recv.con_error as e:
            raise RevisionSqlError('{!r}: {!r}: {}'.format(host_name, type(e), e)) from e
    
    def _guard_revision(self, revision_schema_ident, revision_ident, host_type, revision):
        guard_revision_body = self._guard_revision_sql.format(
            q_revision_schema_ident=self._pg_ident_quote(revision_schema_ident),
            q_revision_ident=self._pg_ident_quote(revision_ident),
            q_application=self._pg_quote(self._application),
            q_schemas_type=self._pg_quote(host_type),
            q_revision=self._pg_quote(revision),
        )
        
        return 'do {};'.format(self._pg_dollar_quote('do', guard_revision_body))
    
    def _clean_revision(self, revision_schema_ident, revision_ident, host_type):
        return self._clean_revision_sql.format(
            q_revision_schema_ident=self._pg_ident_quote(revision_schema_ident),
            q_revision_ident=self._pg_ident_quote(revision_ident),
            q_application=self._pg_quote(self._application),
            q_schemas_type=self._pg_quote(host_type),
        )
    
    def _push_revision(
                self,
                revision_schema_ident,
                revision_ident,
                revision_history_ident,
                host_type,
                revision,
                comment,
                schemas,
            ):
        if schemas is not None:
            q_schemas = 'array[{}\n]'.format(
                ','.join('\n{}'.format(self._pg_quote(x)) for x in schemas),
            )
        else:
            q_schemas = 'null'
        
        return self._push_revision_sql.format(
            q_revision_schema_ident=self._pg_ident_quote(revision_schema_ident),
            q_revision_ident=self._pg_ident_quote(revision_ident),
            q_revision_history_ident=self._pg_ident_quote(revision_history_ident),
            q_application=self._pg_quote(self._application),
            q_schemas_type=self._pg_quote(host_type),
            q_revision=self._pg_quote(revision),
            q_comment=self._pg_quote(comment),
            q_schemas=q_schemas,
        )
    
    def _drop_schemas(self, revision_schema_ident, revision_ident, host_type, schemas):
        if schemas is not None:
            q_schemas = 'array[{}\n]'.format(
                ','.join('\n{}'.format(self._pg_quote(x)) for x in schemas),
            )
        else:
            q_schemas = 'null'
        
        drop_schemas_body = self._drop_schemas_sql.format(
            q_revision_schema_ident=self._pg_ident_quote(revision_schema_ident),
            q_revision_ident=self._pg_ident_quote(revision_ident),
            q_application=self._pg_quote(self._application),
            q_schemas_type=self._pg_quote(host_type),
            q_schemas=q_schemas,
        )
        
        return 'do {};'.format(self._pg_dollar_quote('do', drop_schemas_body))
    
    def ensure_revision_structs(self, host_type):
        application_ident = self._revision_sql_utils.make_ident(self._application)
        host_type_ident = self._revision_sql_utils.make_ident(host_type)
        q_revision_schema_ident = self._pg_ident_quote(
            self._revision_sql_utils.revision_schema_ident(application_ident),
        )
        
        create_list = [
            self._create_revision_schema_sql.format(
                q_revision_schema_ident=q_revision_schema_ident,
            ),
            self._create_revision_table_sql.format(
                q_revision_schema_ident=q_revision_schema_ident,
                q_revision_ident=self._pg_ident_quote(
                    self._revision_sql_utils.var_revision_ident(host_type_ident),
                ),
            ),
            self._create_revision_table_sql.format(
                q_revision_schema_ident=q_revision_schema_ident,
                q_revision_ident=self._pg_ident_quote(
                    self._revision_sql_utils.func_revision_ident(host_type_ident),
                ),
            ),
            self._create_revision_history_table_sql.format(
                q_revision_schema_ident=q_revision_schema_ident,
                q_revision_history_ident=self._pg_ident_quote(
                    self._revision_sql_utils.var_revision_history_ident(host_type_ident),
                ),
            ),
            self._create_revision_history_table_sql.format(
                q_revision_schema_ident=q_revision_schema_ident,
                q_revision_history_ident=self._pg_ident_quote(
                    self._revision_sql_utils.func_revision_history_ident(host_type_ident),
                ),
            ),
        ]
        
        return '\n\n'.join(create_list)
    
    def fetch_var_revision(self, recv, host_name, host_type):
        application_ident = self._revision_sql_utils.make_ident(self._application)
        host_type_ident = self._revision_sql_utils.make_ident(host_type)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.var_revision_ident(host_type_ident)
        
        return self._fetch_revision(
                recv, host_name, revision_schema_ident, revision_ident, host_type)
    
    def fetch_func_revision(self, recv, host_name, host_type):
        application_ident = self._revision_sql_utils.make_ident(self._application)
        host_type_ident = self._revision_sql_utils.make_ident(host_type)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.func_revision_ident(host_type_ident)
        
        return self._fetch_revision(
                recv, host_name, revision_schema_ident, revision_ident, host_type)
    
    def guard_var_revision(self, host_type, revision):
        application_ident = self._revision_sql_utils.make_ident(self._application)
        host_type_ident = self._revision_sql_utils.make_ident(host_type)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.var_revision_ident(host_type_ident)
        
        return self._guard_revision(revision_schema_ident, revision_ident, host_type, revision)
    
    def guard_func_revision(self, host_type, revision):
        application_ident = self._revision_sql_utils.make_ident(self._application)
        host_type_ident = self._revision_sql_utils.make_ident(host_type)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.func_revision_ident(host_type_ident)
        
        return self._guard_revision(revision_schema_ident, revision_ident, host_type, revision)
    
    def clean_var_revision(self, host_type):
        application_ident = self._revision_sql_utils.make_ident(self._application)
        host_type_ident = self._revision_sql_utils.make_ident(host_type)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.var_revision_ident(host_type_ident)
        
        return self._clean_revision(revision_schema_ident, revision_ident, host_type)
    
    def clean_func_revision(self, host_type):
        application_ident = self._revision_sql_utils.make_ident(self._application)
        host_type_ident = self._revision_sql_utils.make_ident(host_type)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.func_revision_ident(host_type_ident)
        
        return self._clean_revision(revision_schema_ident, revision_ident, host_type)
    
    def push_var_revision(self, host_type, revision, comment, schemas):
        application_ident = self._revision_sql_utils.make_ident(self._application)
        host_type_ident = self._revision_sql_utils.make_ident(host_type)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.var_revision_ident(host_type_ident)
        revision_history_ident = self._revision_sql_utils.var_revision_history_ident(host_type_ident)
        
        return self._push_revision(
            revision_schema_ident,
            revision_ident,
            revision_history_ident,
            host_type,
            revision,
            comment,
            schemas,
        )
    
    def push_func_revision(self, host_type, revision, comment, schemas):
        application_ident = self._revision_sql_utils.make_ident(self._application)
        host_type_ident = self._revision_sql_utils.make_ident(host_type)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.func_revision_ident(host_type_ident)
        revision_history_ident = self._revision_sql_utils.func_revision_history_ident(host_type_ident)
        
        return self._push_revision(
            revision_schema_ident,
            revision_ident,
            revision_history_ident,
            host_type,
            revision,
            comment,
            schemas,
        )
    
    def drop_var_schemas(self, host_type, schemas):
        application_ident = self._revision_sql_utils.make_ident(self._application)
        host_type_ident = self._revision_sql_utils.make_ident(host_type)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.var_revision_ident(host_type_ident)
        
        return self._drop_schemas(revision_schema_ident, revision_ident, host_type, schemas)
    
    def drop_func_schemas(self, host_type, schemas):
        application_ident = self._revision_sql_utils.make_ident(self._application)
        host_type_ident = self._revision_sql_utils.make_ident(host_type)
        revision_schema_ident = self._revision_sql_utils.revision_schema_ident(application_ident)
        revision_ident = self._revision_sql_utils.func_revision_ident(host_type_ident)
        
        return self._drop_schemas(revision_schema_ident, revision_ident, host_type, schemas)
