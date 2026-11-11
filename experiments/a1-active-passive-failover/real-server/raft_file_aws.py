#!/usr/bin/env python3
"""Offline integration adapter. Supports file-backed s3 cp only; all other AWS actions fail."""
import os
from pathlib import Path
import shutil
import sys


def main():
    args=sys.argv[1:]
    if args[:2]!=['s3','cp'] or 'OPENKACHE_VALIDATION_S3' not in os.environ:
        raise SystemExit('offline adapter refuses all non-file S3 operations')
    root=Path(os.environ['OPENKACHE_VALIDATION_S3']).resolve()
    def path(value):
        if value.startswith('s3://'):
            candidate=root/value[5:]
            if not candidate.resolve().is_relative_to(root):raise ValueError('unsafe offline key')
            return candidate
        return Path(value)
    source,destination=map(path,args[2:4])
    destination.parent.mkdir(parents=True,exist_ok=True)
    if '--recursive' in args:shutil.copytree(source,destination,dirs_exist_ok=True)
    else:shutil.copyfile(source,destination)


if __name__=='__main__':main()
