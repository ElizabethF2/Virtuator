#!/bin/env python3

import site, os, shutil, tempfile

src = os.path.join(os.path.dirname(__file__), '..', 'virtuator.py')
dst = os.path.join(site.getusersitepackages(), 'virtuator.py')
os.makedirs(site.getusersitepackages(), exist_ok = True)
shutil.copy2(src, dst)

import virtuator
src = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'well_known_vmdefs'))
dst = os.path.join(virtuator.get_data_dir(), 'well_known_vmdefs')
os.makedirs(dst, exist_ok = True)
if os.path.exists(dst):
  shutil.move(dst, os.path.join(tempfile.mkdtemp(prefix='vmdefs'), 'well_known_vmdefs'))
shutil.copytree(src, dst)

