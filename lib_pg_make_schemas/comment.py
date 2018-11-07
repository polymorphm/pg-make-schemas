import subprocess

COMMENT_FILE_NAME = 'comment.sh'

def comment(comment_file_path):
    p = subprocess.run(
        comment_file_path,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        check=True,
    )

    return p.stdout.decode().rstrip()

# vi:ts=4:sw=4:et
