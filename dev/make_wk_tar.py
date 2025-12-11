#!/bin/env python3

import os, tarfile, tempfile, io, hashlib

def main():
  root = os.path.abspath('../well_known_vmdefs')
  tdir = tempfile.mkdtemp(prefix='wk_')
  print('Output:', tdir)
  tarbuf = io.BytesIO()
  with tarfile.open(fileobj = tarbuf, mode = 'w:gz') as tf:
    for fname in os.listdir(root):
      with open(os.path.join(root, fname), 'rb') as f:
        data = f.read()
      info = tarfile.TarInfo(name = fname)
      info.size = len(data)
      tf.addfile(info, io.BytesIO(data))

  tarhash = hashlib.sha256(tarbuf.getvalue())
  print('Hash:', tarhash.hexdigest())
  print('Size:', len(tarbuf.getvalue()))
  with open(os.path.join(tdir, 'well_known_vmdefs.tar.gz'), 'xb') as f:
    f.write(tarbuf.getvalue())

if __name__ == '__main__':
  main()

# tf = tarfile.open('/tmp/wk_7tau3b3d/well_known_vmdefs.tar.gz')
# print(tf.extractfile('arch.vmdef').read().decode())
