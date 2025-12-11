# Generates the manifest used in the code for SYSTEMIMG_MANIFEST and 
# This script automatically retrieves the latest ISO URLs and verifies hashes
# and GPG signatures. This ensures that Virtuator can reliably verify
# systemimgs and firmware images without needing gpg as another dependency.
# All downloads are done over HTTPS with certificate verification enabled.
# Key fingerprint(s) are hardcoded and sources are listed below.

import os, requests, re, shutil, subprocess, tempfile, json, html, hashlib
import urllib.request, tarfile

SKIP_SYSTEMIMGS = False
SKIP_FIRMWARE = True

DL_URL = 'https://alpinelinux.org/downloads'
KEY_FINGERPRINTS = (
  '0482D84022F52DF1C4E7CD43293ACD0907D9495A', # From https://alpinelinux.org/keybase.txt
)
ISO_DEST = None
SYSTEMIMG = 'alpinelatest'
HASH_BUFFER_SIZE = 8388608 # 8 MB

ARCH_MAP = {
  'x86_64': 'x86_64',
  'x86': 'x86',
  'aarch64': 'aarch64',
  'armv7': 'arm',
}

PKG_BASE_URL = 'https://kojipkgs.fedoraproject.org/packages/edk2'

FEDORA_REPO_VERSION = '4.fc41'

PKG_MAP = {
  'x86_64': 'ovmf',
  'aarch64': 'aarch64',
  'arm': 'arm',
}

def hash_file(path):
  h = hashlib.sha256()
  with open(path, 'rb') as f:
    while True:
      buf = f.read(HASH_BUFFER_SIZE)
      if not buf:
        break
      h.update(buf)
  return h.hexdigest()


tdir = tempfile.mkdtemp()
print('Temp Dir:', tdir)
env = dict(os.environ)
env['GNUPGHOME'] = tdir
gpg = shutil.which('gpg')
if not gpg: raise Exception('No gpg')
for key_fingerprint in KEY_FINGERPRINTS:
  print('Importing {} to GNUPGHOME at {}'.format(key_fingerprint, env['GNUPGHOME']))
  # NB: gpg must already know keyservers
  subprocess.check_call((gpg, '--recv-keys', key_fingerprint), env=env)

if not SKIP_SYSTEMIMGS:
  req = requests.get(DL_URL)
  all_links = [html.unescape(i) for i in re.findall(r'href="(https\:.+?)"', req.text)]
  virt_links = list(filter(lambda i: '-virt-' in i, all_links))
  if len(virt_links) != (3*len(ARCH_MAP)): raise Exception()

  manifest = ''
  for arch, march in ARCH_MAP.items():
    print('Processing', march)
    iso_url = list(filter(lambda i: i.endswith('-'+arch+'.iso'), virt_links))[0]
    print('ISO Url:', iso_url)
    sha256_url = list(filter(lambda i: i.endswith('-'+arch+'.iso.sha256'), virt_links))[0]
    print('SHA256 Url:', sha256_url)
    asc_url = list(filter(lambda i: i.endswith('-'+arch+'.iso.asc'), virt_links))[0]
    print('GPG Url:', asc_url)
    m = {'url': iso_url}
    req = requests.get(sha256_url)
    m['sha256'] = req.text.split()[0]
    print('Current Manifest:', m)
    asc_path = os.path.join(tdir, march+'.asc')
    urllib.request.urlretrieve(asc_url, asc_path)
    if ISO_DEST:
      iso_path = os.path.join(ISO_DEST, 'systemimg_'+ SYSTEMIMG + '_'+march+'.iso')
    else:
      iso_path = os.path.join(tdir, march+'.iso')
    urllib.request.urlretrieve(iso_url, iso_path)
    m['size'] = os.path.getsize(iso_path)
    print('Current Manifest:', m)
    hsh = hash_file(iso_path)
    if hsh != m['sha256']: raise Exception('Bad hash')
    subprocess.check_call((gpg, '--verify', asc_path, iso_path), env=env)
    
    manifest += ('  \'' + SYSTEMIMG + '_' + march + '\': {\n')
    manifest += ('     \'url\': \'' + m['url'] + '\',\n')
    manifest += ('     \'sha256\': \'' + m['sha256'] + '\',\n')
    manifest += ('     \'size\': ' + str(m['size']) + ',\n')
    manifest += ('  },\n')

  print('Final Manifest:')
  print(manifest)

if not SKIP_FIRMWARE:
  print('Checking:', PKG_BASE_URL)
  req = requests.get(PKG_BASE_URL)
  latest_url = None
  latest_mtime = ''
  for link, modified in re.findall(r'href="(.+?)".+?</a>([\s\d\-\:]+)', req.text):
    match = re.match(r'\s*(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2})\s*-?\s*', modified)
    if match:
      mtime = match.group(1)
      if mtime > latest_mtime:
        latest_url = link
        latest_mtime = mtime
  version = latest_url.split('/')[0]
  print('Found latest:', version)
  burl = PKG_BASE_URL
  burl += ('/' if not burl.endswith('/') else '') + latest_url
  burl += ('/' if not burl.endswith('/') else '') + FEDORA_REPO_VERSION
  burl += ('/' if not burl.endswith('/') else '') + 'noarch'
  for arch, parch in PKG_MAP.items():
    rpm_name = 'edk2-{}-{}-{}.noarch.rpm'.format(parch, version, FEDORA_REPO_VERSION)
    url = '{}/{}'.format(burl, rpm_name)
    rpm_path = os.path.join(tdir, 'edk2-{}.rpm'.format(arch))
    print('Downloading', rpm_path)
    print('Incomplete / Work in Progress!')
    breakpoint()
    urllib.request.urlretrieve(url, rpm_path)
