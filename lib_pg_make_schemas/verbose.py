# -*- mode: python; coding: utf-8 -*-

class NonVerbose:
    def begin_host(self, host_name):
        pass
    
    def finish_host(self, host_name):
        pass

class Verbose:
    def __init__(self, print_func, err_print_func):
        self._print_func = print_func
        self._err_print_func = err_print_func
    
    def begin_host(self, host_name):
        self._print_func('{!r}: beginning...'.format(host_name))
    
    def finish_host(self, host_name):
        self._print_func('{!r}: finishing...'.format(host_name))

def make_verbose(print_func, err_print_func, verbose):
    if not verbose:
        return NonVerbose()
    
    return Verbose(print_func, err_print_func)
