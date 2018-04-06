# -*- mode: python; coding: utf-8 -*-

import subprocess

def comment(comment_file_path):
    p = subprocess.run(
        comment_file_path,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        check=True,
    )
    
    return p.stdout.decode().rstrip()
