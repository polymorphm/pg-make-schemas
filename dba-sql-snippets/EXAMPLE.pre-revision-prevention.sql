-- this is an example how to prevent accidental undesirable upgrading
-- to PRE-revision on production database host.
--
-- substitute APPLICATION_NAME and APPLICATION_SCHEMAS_TYPE
-- to your real values of your project.

--begin;

set local role to postgres;
set local search_path to '';

alter table APPLICATION_NAME_revision.APPLICATION_SCHEMAS_TYPE_var_revision
        add constraint pre_revision_prevention_check
        check (revision !~~* 'PRE-%');

--commit;

-- vi:ts=4:sw=4:et
