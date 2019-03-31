-- function to migrate old revision-keeping-structures into new ones

create or replace function pg_temp.pg_make_schemas_migrate_to_4_from_3 (
            _application text,
            _schemas_type text
        )
        returns void
        language plpgsql
as $function$declare
    __application_ident text;
    __schemas_type_ident text;
    __revision_schema_ident text;
    __var_revision_ident text;
    __func_revision_ident text;
    __var_revision_history_ident text;
    __func_revision_history_ident text;
    __old_var_revision_ident text;
    __old_func_revision_ident text;
    __old_var_revision_history_ident text;
    __old_func_revision_history_ident text;
begin
    if _application is null or _schemas_type is null then
        raise 'invalid args';
    end if;
    
    __application_ident := lower (replace (_application, '-', '_'));
    __schemas_type_ident := lower (replace (_schemas_type, '-', '_'));
    
    __revision_schema_ident := __application_ident || '_revision';
    __var_revision_ident := __schemas_type_ident || '_var_revision';
    __func_revision_ident := __schemas_type_ident || '_func_revision';
    __var_revision_history_ident := __schemas_type_ident || '_var_revision_history';
    __func_revision_history_ident := __schemas_type_ident || '_func_revision_history';
    
    __old_var_revision_ident := __application_ident || '_var_revision';
    __old_func_revision_ident := __application_ident || '_func_revision';
    __old_var_revision_history_ident := 'arch_' || __old_var_revision_ident;
    __old_func_revision_history_ident := 'arch_' || __old_func_revision_ident;
    
    execute format (
        $dyn_sql$
            lock %1$I.%6$I, %1$I.%7$I, %1$I.%8$I, %1$I.%9$I;
            
            create table if not exists %1$I.%2$I (
                application text not null,
                schemas_type text not null,
                primary key (application, schemas_type),
                datetime timestamp with time zone not null,
                revision text not null,
                comment text,
                schemas text[]
            );
            
            create table if not exists %1$I.%3$I (
                application text not null,
                schemas_type text not null,
                primary key (application, schemas_type),
                datetime timestamp with time zone not null,
                revision text not null,
                comment text,
                schemas text[]
            );
            
            create table if not exists %1$I.%4$I (
                revision_history_id bigserial primary key,
                application text not null,
                schemas_type text not null,
                unique (application, schemas_type, revision_history_id),
                datetime timestamp with time zone not null,
                revision text not null,
                comment text,
                schemas text[]
            );
            
            create table if not exists %1$I.%5$I (
                revision_history_id bigserial primary key,
                application text not null,
                schemas_type text not null,
                unique (application, schemas_type, revision_history_id),
                datetime timestamp with time zone not null,
                revision text not null,
                comment text,
                schemas text[]
            );
            
            with del_old as (
                        delete from %1$I.%8$I old
                                where old.application = $1
                                returning
                                    old.id,
                                    old.application,
                                    $2 schemas_type,
                                    old.datetime,
                                    old.revision,
                                    old.comment,
                                    old.schemas
                    )
                    insert into %1$I.%4$I (
                        application,
                        schemas_type,
                        datetime,
                        revision,
                        comment,
                        schemas
                    )
                    select
                        o.application,
                        o.schemas_type,
                        o.datetime,
                        o.revision,
                        o.comment,
                        o.schemas
                    from del_old o
                    order by o.id;
            
            with del_old as (
                        delete from %1$I.%9$I old
                                where old.application = $1
                                returning
                                    old.id,
                                    old.application,
                                    $2 schemas_type,
                                    old.datetime,
                                    old.revision,
                                    old.comment,
                                    old.schemas
                    )
                    insert into %1$I.%5$I (
                        application,
                        schemas_type,
                        datetime,
                        revision,
                        comment,
                        schemas
                    )
                    select
                        o.application,
                        o.schemas_type,
                        o.datetime,
                        o.revision,
                        o.comment,
                        o.schemas
                    from del_old o
                    order by o.id;
            
            with del_old as (
                        delete from %1$I.%6$I old
                                where old.application = $1
                                returning
                                    old.application,
                                    $2,
                                    old.datetime,
                                    old.revision,
                                    old.comment,
                                    old.schemas
                    )
                    ,
                    ins_rev as (
                        insert into %1$I.%2$I (
                                    application,
                                    schemas_type,
                                    datetime,
                                    revision,
                                    comment,
                                    schemas
                                )
                                select o.* from del_old o
                                returning *
                    )
                    insert into %1$I.%4$I (
                        application,
                        schemas_type,
                        datetime,
                        revision,
                        comment,
                        schemas
                    )
                    select
                        rev.application,
                        rev.schemas_type,
                        rev.datetime,
                        rev.revision,
                        rev.comment,
                        rev.schemas
                    from ins_rev rev;
            
            with del_old as (
                        delete from %1$I.%7$I old
                                where old.application = $1
                                returning
                                    old.application,
                                    $2,
                                    old.datetime,
                                    old.revision,
                                    old.comment,
                                    old.schemas
                    )
                    ,
                    ins_rev as (
                        insert into %1$I.%3$I (
                                    application,
                                    schemas_type,
                                    datetime,
                                    revision,
                                    comment,
                                    schemas
                                )
                                select o.* from del_old o
                                returning *
                    )
                    insert into %1$I.%5$I (
                        application,
                        schemas_type,
                        datetime,
                        revision,
                        comment,
                        schemas
                    )
                    select
                        rev.application,
                        rev.schemas_type,
                        rev.datetime,
                        rev.revision,
                        rev.comment,
                        rev.schemas
                    from ins_rev rev;
        $dyn_sql$,
        __revision_schema_ident,            -- %1$I
        __var_revision_ident,               -- %2$I
        __func_revision_ident,              -- %3$I
        __var_revision_history_ident,       -- %4$I
        __func_revision_history_ident,      -- %5$I
        __old_var_revision_ident,           -- %6$I
        __old_func_revision_ident,          -- %7$I
        __old_var_revision_history_ident,   -- %8$I
        __old_func_revision_history_ident   -- %9$I
    ) using
        _application,                       -- $1
        _schemas_type;                      -- $2
end$function$;

-- vi:ts=4:sw=4:et
