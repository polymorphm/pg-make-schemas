# -*- mode: python; coding: utf-8 -*-

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
    
    def _connect(self, conninfo):
        return psycopg2.connect(conninfo)
    
    def _open(self, output_path):
        return open(output_path, 'w', encoding='utf-8', newline='\n')
    
    def begin(self, hosts_descr):
        if self._execute:
            for host in hosts_descr.host_list:
                host_name = host['name']
                conninfo = host['conninfo']
                
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
            for host in hosts_descr.host_list:
                host_name = host['name']
                host_type = host['type']
                conninfo = host['conninfo']
                
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
            
            for host in hosts_descr.host_list:
                host_name = host['name']
                fd = self._fd_map[host_name]
                
                self._sql_file_utils.write_header(fd)
    
    def get_con(self, host_name):
        if self._execute:
            return self._con_map[host_name]
    
    def write_fragment(self, host_name, fragment):
       if self._output is not None:
            fd = self._fd_map[host_name]
            
            self._sql_file_utils.write_fragment(fd, fragment)
    
    def execute(self, host_name, fragment):
        if self._execute:
            con = self._con_map[host_name]
            
            with con.cursor() as cur:
                try:
                    cur.execute(fragment)
                except self.con_error as e:
                    raise ReceiversError('{!r}: {!r}: {}'.format(host_name, type(e), e)) from e
        
        self.write_fragment(host_name, fragment)
    
    def done(self, hosts_descr):
        if self._execute:
            for host in hosts_descr.host_list:
                host_name = host['name']
                con = self._con_map[host_name]
                
                if self._pretend:
                    con.rollback()
                else:
                    con.commit()
        
        if self._output is not None:
            for host in hosts_descr.host_list:
                host_name = host['name']
                fd = self._fd_map[host_name]
                
                self._sql_file_utils.write_footer(fd)
            
            for host in reversed(hosts_descr.host_list):
                host_name = host['name']
                fd = self._fd_map[host_name]
                
                fd.close()
                del self._fd_map[host_name]
        
        if self._execute:
            for host in reversed(hosts_descr.host_list):
                host_name = host['name']
                con = self._con_map[host_name]
                
                con.close()
                del self._con_map[host_name]
    
    def close(self):
        for host_name, fd in reversed(list(self._fd_map.items())):
            fd.close()
            del self._fd_map[host_name]
        
        for host_name, con in reversed(list(self._con_map.items())):
            con.close()
            del self._con_map[host_name]
