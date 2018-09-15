# -*- mode: python; coding: utf-8 -*-

class NonVerbose:
    def prepare_init(self):
        pass
    
    def prepare_install(self):
        pass
    
    def prepare_upgrade(self):
        pass
    
    def source_code_revision(self, application, revision, comment):
        pass
    
    def begin_host(self, host_name):
        pass
    
    def scr_env(self, host_name, fragment_i):
        pass
    
    def ensure_revision_structs(self, host_name, fragment_i):
        pass
    
    def guard_var_revision(self, host_name, revision, fragment_i):
        pass
    
    def guard_func_revision(self, host_name, revision, fragment_i):
        pass
    
    def clean_var_revision(self, host_name, fragment_i):
        pass
    
    def clean_func_revision(self, host_name, fragment_i):
        pass
    
    def push_var_revision(self, host_name, revision, comment, fragment_i):
        pass
    
    def push_func_revision(self, host_name, revision, comment, fragment_i):
        pass
    
    def drop_var_schemas(self, host_name, cascade, fragment_i):
        pass
    
    def drop_func_schemas(self, host_name, cascade, fragment_i):
        pass
    
    def create_schema(self, host_name, schema_name, fragment_i):
        pass
    
    def guard_acls(self, host_name, schema_name, fragment_i):
        pass
    
    def execute_sql(self, host_name, script_type, fragment_i):
        pass
    
    def clean_scr_env(self, host_name, fragment_i):
        pass
    
    def finish_host(self, host_name):
        pass

class Verbose:
    def __init__(self, print_func, err_print_func):
        self._print_func = print_func
        self._err_print_func = err_print_func
    
    def _format_frag(self, fragment_i):
        return 'since fragment {!r}'.format(fragment_i)
    
    def prepare_init(self):
        self._print_func('preparing for initialization...')
    
    def prepare_install(self):
        self._print_func('preparing for installing...')
    
    def prepare_upgrade(self):
        self._print_func('preparing for upgrading...')
    
    def source_code_revision(self, application, revision, comment):
        self._print_func(
            'application {!r}: source code has revision {!r}{}'.format(
                application,
                revision,
                ' comment {!r}'.format(comment)
                        if comment is not None else '',
            ),
        )
    
    def begin_host(self, host_name):
        self._print_func('{!r}: beginning...'.format(host_name))
    
    def scr_env(self, host_name, fragment_i):
        self._print_func(
            '{!r}: making script environment ({})...'.format(
                host_name,
                self._format_frag(fragment_i),
            ),
        )
    
    def ensure_revision_structs(self, host_name, fragment_i):
        self._print_func(
            '{!r}: ensuring revision structures ({})...'.format(
                host_name,
                self._format_frag(fragment_i),
            ),
        )
    
    def guard_var_revision(self, host_name, revision, fragment_i):
        self._print_func(
            '{!r}: guarding var revision {!r} ({})...'.format(
                host_name,
                revision,
                self._format_frag(fragment_i),
            ),
        )
    
    def guard_func_revision(self, host_name, revision, fragment_i):
        self._print_func(
            '{!r}: guarding func revision {!r} ({})...'.format(
                host_name,
                revision,
                self._format_frag(fragment_i),
            ),
        )
    
    def clean_var_revision(self, host_name, fragment_i):
        self._print_func(
            '{!r}: cleaning var revision ({})...'.format(
                host_name,
                self._format_frag(fragment_i),
            ),
        )
    
    def clean_func_revision(self, host_name, fragment_i):
        self._print_func(
            '{!r}: cleaning func revision ({})...'.format(
                host_name,
                self._format_frag(fragment_i),
            ),
        )
    
    def push_var_revision(self, host_name, revision, comment, fragment_i):
        self._print_func(
            '{!r}: pushing var revision {!r}{} ({})...'.format(
                host_name,
                revision,
                ' comment {!r}'.format(comment)
                        if comment is not None else '',
                self._format_frag(fragment_i),
            ),
        )
    
    def push_func_revision(self, host_name, revision, comment, fragment_i):
        self._print_func(
            '{!r}: pushing func revision {!r}{} ({})...'.format(
                host_name,
                revision,
                ' comment {!r}'.format(comment)
                        if comment is not None else '',
                self._format_frag(fragment_i),
            ),
        )
    
    def drop_var_schemas(self, host_name, cascade, fragment_i):
        self._print_func(
            '{!r}: {} dropping var schemas ({})...'.format(
                host_name,
                'cascaded' if cascade else 'safe',
                self._format_frag(fragment_i),
            ),
        )
    
    def drop_func_schemas(self, host_name, cascade, fragment_i):
        self._print_func(
            '{!r}: {} dropping func schemas ({})...'.format(
                host_name,
                'cascaded' if cascade else 'safe',
                self._format_frag(fragment_i),
            ),
        )
    
    def create_schema(self, host_name, schema_name, fragment_i):
        self._print_func(
            '{!r}: creating schema {!r} ({})...'.format(
                host_name,
                schema_name,
                self._format_frag(fragment_i),
            ),
        )
    
    def guard_acls(self, host_name, schema_name, fragment_i):
        self._print_func(
            '{!r}: guarding acls for schema {!r} ({})...'.format(
                host_name,
                schema_name,
                self._format_frag(fragment_i),
            ),
        )
    
    def execute_sql(self, host_name, script_type, fragment_i):
        script_title_map = {
            'init_sql': 'initialization',
            'var_install_sql': 'var installing',
            'late_install_sql': 'late installing',
            'func_install_sql': 'func installing',
            'upgrade_sql': 'upgrading',
            'settings_sql': 'settings installing',
            'settings_upgrade_sql': 'settings upgrading',
            'safeguard_sql': 'safeguard',
        }
        
        self._print_func(
            '{!r}: executing {} scripts ({})...'.format(
                host_name,
                script_title_map[script_type],
                self._format_frag(fragment_i),
            ),
        )
    
    def clean_scr_env(self, host_name, fragment_i):
        self._print_func(
            '{!r}: cleaning script environment ({})...'.format(
                host_name,
                self._format_frag(fragment_i),
            ),
        )
    
    def finish_host(self, host_name):
        self._print_func('{!r}: finishing...'.format(host_name))

def make_verbose(print_func, err_print_func, verbose):
    if not verbose:
        return NonVerbose()
    
    return Verbose(print_func, err_print_func)
