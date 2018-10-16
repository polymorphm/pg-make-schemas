# -*- mode: python; coding: utf-8 -*-

import itertools
import psycopg2

class ReceiversError(Exception):
    pass

class SqlFileUtils:
    @classmethod
    def write_header(cls, fd):
        fd.write('-- -*- mode: sql; coding: utf-8 -*-\n\n--begin;\n\n')
        fd.flush()
    
    @classmethod
    def write_fragment(cls, fd, fragment):
        fd.write(fragment)
        fd.write('\n\n')
        fd.flush()
    
    @classmethod
    def write_fragment_ok_notice(cls, fd, frag_cnt):
        fragment_i = next(frag_cnt)
        
        fd.write(
                'do $do$begin raise notice \'fragment {}: ok\'; end$do$;'.format(
                int(fragment_i),
            ),
        )
        fd.write('\n\n')
        fd.flush()
    
    @classmethod
    def write_footer(cls, fd):
        fd.write('--commit;\n')
        fd.flush()

class Receivers:
    _sql_file_utils = SqlFileUtils
    
    con_error = psycopg2.Error
    
    def __init__(self, execute, pretend, output):
        self._execute = execute
        self._pretend = pretend
        self._output = output
        self._con_map = {}
        self._fd_map = {}
        self._frag_cnt_map = {}
    
    def _connect(self, conninfo):
        return psycopg2.connect(conninfo)
    
    def _open(self, output_path):
        return open(output_path, 'w', encoding='utf-8', newline='\n')
    
    def _make_counter(self, restore_value=None):
        if restore_value is None:
            restore_value = 1
        
        return itertools.count(restore_value)
    
    def begin_host(self, hosts_descr, host):
        host_name = host['name']
        host_type = host['type']
        conninfo = host['conninfo']
        
        if self._execute:
            if host_name in self._con_map:
                raise ValueError(
                    '{!r}, {!r}: non unique host_name'.format(
                        host_name,
                        hosts_descr.hosts_file_path,
                    ),
                )
            
            if conninfo is None:
                raise ValueError(
                    '{!r}, {!r}: unable to connect to host without its conninfo'.format(
                        host_name,
                        hosts_descr.hosts_file_path,
                    ),
                )
            
            con = self._connect(conninfo)
            self._con_map[host_name] = con
        
        if self._output is not None:
            if host_name in self._fd_map:
                raise ValueError(
                    '{!r}, {!r}: non unique host_name'.format(
                        host_name,
                        hosts_descr.hosts_file_path,
                    ),
                )
            
            output_path = '{}.{}.{}.sql'.format(
                self._output,
                host_name.replace('/', '-').replace('.', '-'),
                host_type.replace('/', '-').replace('.', '-'),
            )
            
            fd = self._open(output_path)
            self._fd_map[host_name] = fd
            self._frag_cnt_map[host_name] = self._make_counter()
            
            self._sql_file_utils.write_header(fd)
    
    def begin(self, hosts_descr, begin_host_verb_func=None):
        for host in hosts_descr.host_list:
            if begin_host_verb_func is not None:
                host_name = host['name']
                
                begin_host_verb_func(host_name)
            
            self.begin_host(hosts_descr, host)
    
    def get_con(self, host_name):
        if self._execute:
            return self._con_map[host_name]
    
    def look_fragment_i(self, host_name):
        frag_cnt = self._frag_cnt_map.get(host_name)
        
        if frag_cnt is None:
            return
        
        fragment_i = next(frag_cnt)
        frag_cnt = self._make_counter(restore_value=fragment_i)
        self._frag_cnt_map[host_name] = frag_cnt
        
        return fragment_i
    
    def write_fragment(self, host_name, fragment):
       if self._output is not None:
            fd = self._fd_map[host_name]
            
            self._sql_file_utils.write_fragment(fd, fragment)
    
    def write_fragment_ok_notice(self, host_name):
       if self._output is not None:
            fd = self._fd_map[host_name]
            frag_cnt = self._frag_cnt_map[host_name]
            
            self._sql_file_utils.write_fragment_ok_notice(fd, frag_cnt)
    
    def execute(self, host_name, fragment):
        self.write_fragment(host_name, fragment)
        
        if self._execute:
            con = self._con_map[host_name]
            
            try:
                with con.cursor() as cur:
                    cur.execute(fragment)
            except self.con_error as e:
                raise ReceiversError('{!r}: {!r}: {}'.format(host_name, type(e), e)) from e
        
        self.write_fragment_ok_notice(host_name)
    
    def finish_host(self, hosts_descr, host):
        host_name = host['name']
        
        if self._execute:
            con = self._con_map[host_name]
            
            if self._pretend:
                con.rollback()
            else:
                con.commit()
        
        if self._output is not None:
            fd = self._fd_map[host_name]
            
            self._sql_file_utils.write_footer(fd)
            
            fd.close()
            del self._fd_map[host_name]
        
        if self._execute:
            con = self._con_map[host_name]
            
            con.close()
            del self._con_map[host_name]
    
    def finish(self, hosts_descr, finish_host_verb_func=None):
        for host in hosts_descr.host_list:
            if finish_host_verb_func is not None:
                host_name = host['name']
                
                finish_host_verb_func(host_name)
            
            self.finish_host(hosts_descr, host)
    
    def close(self):
        for host_name, fd in reversed(list(self._fd_map.items())):
            fd.close()
            del self._fd_map[host_name]
        
        for host_name, con in reversed(list(self._con_map.items())):
            con.close()
            del self._con_map[host_name]
