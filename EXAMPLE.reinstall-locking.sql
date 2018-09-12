-- -*- mode: sql; coding: utf-8 -*-

-- this is just example how to safeguard a production database host
-- against an accidental undesirable reinstalling action with losing data

--begin;

set local role to postgres;
set local search_path to '';

create schema reinstall_locking;
grant usage on schema reinstall_locking to public;

create or replace function reinstall_locking.XXX_YYY_ZZZ_EXAMPLE_reinstall_locking ()
        returns event_trigger
        language plpgsql
        security definer
as $function$begin
    perform 'XXX_EXAMPLE_SCHEMA.SOME_EXAMPLE_TABLE'::regclass;
    perform 'XXX_EXAMPLE_SCHEMA.ANOTHER_EXAMPLE_TABLE'::regclass;
    perform 'YYY_EXAMPLE_SCHEMA.YET_ANOTHER_EXAMPLE_TABLE'::regclass;
    perform 'ZZZ_EXAMPLE_SCHEMA.AND_YET_ANOTHER_EXAMPLE_TABLE'::regclass;
end$function$;

grant execute on
        function reinstall_locking.XXX_YYY_ZZZ_EXAMPLE_reinstall_locking ()
        to public;

create event trigger XXX_YYY_ZZZ_EXAMPLE_reinstall_locking
        on ddl_command_end 
        execute procedure reinstall_locking.XXX_YYY_ZZZ_EXAMPLE_reinstall_locking ();

--commit;
