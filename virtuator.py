#!/usr/bin/env python3

'''
a tool for automatically building and managing VMs

Virtuator is a virtual machine management utility 

If you are packaging Virtuator, see the docstring attached to
_apply_packager_overwrites()

'''

import sys, platform, io, os, stat, errno, time
import subprocess, shutil, shlex, signal, socket
import types, string, collections, functools, threading
import builtins, inspect
import re, json, base64, random, zipfile, hashlib, tomllib, getpass

try:
  import fcntl
  is_unix = True
except ModuleNotFoundError:
  import ctypes, ctypes.wintypes
  is_unix = False

__all__ = [
  'CTRL_A', 'CTRL_C', 'acpi_shutdown', 'basic_shell',
  'disk_from', 'ensure_booted', 'ensure_shell', 'generate_id', 'get_arch',
  'get_backend', 'get_config', 'get_data_dir', 'get_file', 'get_file_data',
  'get_per_vm_data_dir', 'get_systemimg', 'get_systemimg_path',
  'get_verbosity', 'handle', 'hash_file', 'human_size',
  'list_running_vms', 'make_disk_path', 'make_system_shell_helpers',
  'manual_boot', 'pipe_shell', 'pipe_string', 'prepend_system_shell_helpers',
  'put_file', 'put_file_data', 'read_output', 'read_until', 'realtime_shell',
  'run_command', 'send_keys', 'try_find_clean_disk',
  'vmdef_exists', 'vmdef_from', 'vprint', 'wait_for', 'wait_until_stopped',
  'run', 'list_all_vms', 'list_registered_vmdefs', 'list_well_known_vmdefs',
  'list_all_vmdefs', 'get_custom_parameter', 'wrap_with_custom_parameters',
  'force_stop', 'validate_name', 'clear_history', 'get_machine',
  'get_tpm_backend', 'is_running', 'super', 'inherit', 'inherit_all',
  'get_forwarded_ports', 'merge_ports', 'die', 'export',
]

# TODO
PACKAGER_OVERWRITABLE_DOWNLOAD_MANIFEST = {
  'well_known_vmdefs_url': 'x',
  'well_known_vmdefs_extension': '.tar.gz',
  'well_known_vmdefs_sha256':
    '7b7f10de2a344fe7e37be3c9e528b35cb8df8b7c8a0e380e2016d9771bc8f0f1',
  'well_known_vmdefs_size': 15155,
  
  'qemu_firmware_url': 'x',
  'qemu_firmware_extension': '.zip',
  'qemu_firmware_sha256': '',
  'qemu_firmware_size': 0,
}

# Manifest metadata is generated via dev/latest_manifest.py
# Buildable entries are built via well_known_vmdefs/virtuator_builder.vmdef
# See the source for both to audit/verify dependencies

PACKAGER_OVERWRITABLE_SYSTEMIMG_MANIFEST = {
  'alpinelatest_x86_64_url':
    'https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-virt-3.20.1-x86_64.iso',
  'alpinelatest_x86_64_extension': '.iso',
  'alpinelatest_x86_64_sha256':
    'f87a0fd3ab0e65d2a84acd5dad5f8b6afce51cb465f65dd6f8a3810a3723b6e4',
  'alpinelatest_x86_64_size': 63963136,

  'alpinelatest_x86_url':
    'https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86/alpine-virt-3.20.1-x86.iso',
  'alpinelatest_x86_extension': '.iso',
  'alpinelatest_x86_sha256':
    '6f066cd81b29b4778d7194255d97a51197fe290ea17d668b4ddcb649ec7cfa74',
  'alpinelatest_x86_size': 49283072,
  
  'alpinelatest_aarch64_url':
    'https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/aarch64/alpine-virt-3.20.1-aarch64.iso',
  'alpinelatest_aarch64_extension': '.iso',
  'alpinelatest_aarch64_sha256':
    'ca2f0e8aa7a1d7917bce7b9e7bd413772b64ec529a1938d20352558f90a5035a',
  'alpinelatest_aarch64_size': 72722432,

  'alpinelatest_arm_url':
    'https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/armv7/alpine-virt-3.20.1-armv7.iso',
  'alpinelatest_arm_extension': '.iso',
  'alpinelatest_arm_sha256':
    '4bfaea171a4c763ac91a10eb10ed2784a0e0e63a7e2917f9e4f2e635c4ae03fd',
  'alpinelatest_arm_size': 45967360,

  'alpinelatest_riscv_buildable': True,
}

PACKAGER_OVERWRITABLE_FIRMWARE_MANIFEST_PATHS = [
  '/usr/share/qemu/firmware',
  '%PROGRAMFILES%/qemu/share/firmware',
]

# TODO: static builds, ppc, other arch
PACKAGER_OVERWRITABLE_QEMU_BACKENDS = {
  'x86_64': ['qemu-system-x86_64',],
  'x86': ['qemu-system-i386',],
  'arm': ['qemu-system-arm',],
  'aarch64': ['qemu-system-aarch64',],
  'riscv': ['qemu-system-riscv',],
}

# TODO virtualbox?
PACKAGER_OVERWRITABLE_BACKENDS = [PACKAGER_OVERWRITABLE_QEMU_BACKENDS,]

PACKAGER_OVERWRITABLE_TPM_BACKENDS = ['swtpm',]

PACKAGER_OVERWRITABLE_WELL_KNOWN_VMDEF_DIR = None
PACKAGER_OVERWRITABLE_SELF_UPDATES_DISABLED_ERROR = None

def _apply_packager_overwrites():
  r'''
  Variables prefixed with PACKAGER_OVERWRITABLE_ are designed to be easily
  patched/replaced using tools such as patch, sed or python itself. These
  values will always follow these rules:

    - names and keys will not change between versions unless necessary
    - key names will be single quoted
    - list and dictionary items will always end in a comma
    - variable and key names may not be on the same line as their value
    - whitespace may vary between versions

  Some examples of statically replacing values via Python:

  # Replace the a value within a dictionary
  key = 'well_known_vmdefs_sha256'
  new_value = 'foo'
  import re
  new_code = re.sub(fr'(?s)\'{key}\'\s*:\s*\'.+?\',',
                    f'\'{key}\': \'{new_value}\',',
                    old_code)

  # Add a value to the start of a list
  list_name = 'PACKAGER_OVERWRITABLE_FIRMWARE_MANIFEST_PATHS'
  new_value = 'foo'
  import re
  new_code = re.sub(fr'(?s){list_name}\s*=\s*\[\s*',
                    f'{list_name} = [ \'{new_value}\', ',
                    old_code)
                    
  # Replace all values in a list
  list_name = 'PACKAGER_OVERWRITABLE_FIRMWARE_MANIFEST_PATHS'
  new_values = ['foo', 'bar']
  new_code = re.sub(fr'(?s){list_name}\s*=\s*\[.+?]',
                    f'{list_name} = {repr(new_values)}',
                    old_code)

  _apply_packager_overwrites is called multiple times before any overwritable
  values are used. Replace the comment below with code which overwrites the
  value(s) you'd like changed if you need to be able to dynamically replace
  values.
  '''
  # PACKAGER_OVERWRITABLE_APPLY_PACKAGER_OVERWRITES_BODY
  pass

NORMALIZED_ARCHS = {
  'amd64': 'x86_64',
  'x86-64': 'x86_64',
  'arm64': 'aarch64',
  'powerpc': 'ppc',
}

DEFAULT_SYSTEMIMG = 'alpinelatest'

RPC_SERVER_CLASS = 'Virtuator-RPC-Server-Daemon'
RPC_CLIENT_CLASS_PREFIX = 'Virtuator-RPC-Client-'
RPC_MAGIC = 0x62A6793F
INFINITE = 0xFFFFFFFF
WAIT_FAILED = 0xFFFFFFFF
WM_COPYDATA = 0x004A
WM_QUIT = 0x0012
PM_REMOVE = 0x0001

CTRL_A = chr(1)
CTRL_C = chr(3)

DEFAULT_CONFIG = {
  'rpc_server_startup_timeout': 300,
  'rpc_server_startup_polling_delay': 0.1,
  'rpc_idle_timeout': 99,
  'disk_buffer_size': 8388608, # 8 MB
  'socket_buffer_size': 262144, # 256 KB
  'default_stdio_buffer_size': 262144, # 256 KB
  'max_stdio_buffer_size': 26214400, # 25 MB
  'cache_clean_copies_of_vms': False, # TODO figure out when/how to use cache
  'shell_exit_phrase': 'vexit',
  'realtime_shell_phrase': 'vreal',
  'default_verbosity': 3,
  'vmdef_search_paths': [],
  'prefer_user_provided_vmdefs_over_well_known_vmdefs': False,
  'use_whpx': False,
}

# TODO: add __str__ and named members to each exception
class MissingDependencyError(Exception): pass
class AlreadyBootedError(Exception): pass
class StillBuildingError(Exception): pass
class VmExistsError(Exception): pass
class VmNotFoundError(Exception): pass
class VmdefNotFoundError(Exception): pass
class RpcTimoutError(Exception): pass
class RpcInterruptedError(Exception): pass
class RpcUndeserializableError(Exception): pass
class IncompatibleBackendsError(Exception): pass

class VMProxy(object):
  def __init__(self, name, custom_parameters = None):
    self.name = name
    if custom_parameters is not None:
      self.custom_parameters = types.MappingProxyType(custom_parameters)

  def __getattr__(self, a):
    attr = globals()[a]
    params = inspect.signature(attr).parameters
    has_kw = any((p.kind == inspect.Parameter.VAR_KEYWORD
                  for p in params.values()))
    def wrapper(*args, **kwargs):
      for i, default in (('name', None), ('custom_parameters', {})):
        if has_kw or i in params:
          kwargs[i] = getattr(self, i, default)
      return attr(*args, **kwargs)
    return wrapper

class CalledProcessError(Exception):
  def __init__(self, ret, cmd, inp, out):
    self.returncode = ret
    self.cmd = cmd
    self.input = inp
    self.output = out
    self.stdout = out
    self.stderr = None

  def __str__(self):
    str_func = getattr(subprocess.CalledProcessError, '__str__')
    if str_func:
      return str_func(self)
    return ("Command {} returned non-zero exit status {}."
              .format(repr(self.cmd), self.returncode))

_private = {'lock': threading.Lock()}

def generate_id(length = 45):
  '''
  Generate a unique identifier of the specified length. IDs are generated using
  the strongest random number generator on the device and, at the default
  length, have enough entropy to be both used in place of UUIDs and to be
  considered cryptographically secure. Generated IDs always start with a letter
  and contain a mix of letters and digits. As such, they can be safely used
  across various operating systems for a variety of purposes including, but not
  limited to, posix shell script variables, batch script variables, file names,
  environment variable names, user names, passwords, hostnames, URLs, query
  string parameter names and Virtuator VM names.

  :param length: The length of the generated identifier
  :return: The generated identifier as a string
  '''
  if length < 1:
    raise ValueError('length {} too short'.format(repr(length)))
  _id = ''
  available = string.ascii_letters
  while len(_id) < length:
    if not (c := getrandom(1, getattr(os, 'GRND_RANDOM', 0)) \
                 if (getrandom := getattr(os, 'getrandom', None)) else \
                 os.urandom(1)):
      continue
    if (c := chr(ord(c))) not in available:
      continue
    _id += c
    if len(_id) == 1:
      available += string.digits
  return _id

_VALID_NAME_CHARS = string.ascii_letters + string.digits + '_-.'
_MAX_NAME_LENGTH = 300

def validate_name(name, raise_exception_if_invalid = True):
  if type(name) is not str:
    raise TypeError('invalid type for name: {}'.format(repr(name)))
  invalid = any((c not in _VALID_NAME_CHARS for c in name))
  if not name or len(name) > _MAX_NAME_LENGTH:
    invalid = True
  if invalid and raise_exception_if_invalid:
    raise ValueError('Invalid name: {}'.format(shlex.quote(name)))
  return not invalid

@functools.cache
def get_config():
  path = os.environ.get('VIRTUATOR_CONFIG')
  if not path:
    xdg_config_home = os.environ.get('XDG_CONFIG_HOME')
    if xdg_config_home:
      path = os.path.join(xdg_config_home, 'virtuator.toml')
  if not path:
    appdata = os.environ.get('APPDATA')
    if appdata:
      path = os.path.join(appdata, 'virtuator.toml')
  if not path:
    path = os.path.expanduser(os.path.join('~', '.config', 'virtuator.toml'))
  try:
    with open(path, 'rb') as f:
      config = tomllib.load(f)
  except FileNotFoundError:
    config = {}
  for k, v in DEFAULT_CONFIG.items():
    if k not in config:
      config[k] = v
  return config

def _C(name):
  return get_config().get(name)

def get_data_dir():
  path = _C('data_dir')
  if path:
    return path
  data_dir = os.environ.get('XDG_DATA_DIR')
  if not data_dir:
    data_dir = os.environ.get('LOCALAPPDATA')
  if not data_dir:
    data_dir = os.path.expanduser('~/.local/share')
  d = os.path.join(data_dir, 'virtuator')
  return d

_VM_DIR_SUFFIXES = ('data', 'tpm_state')

def _get_per_vm_dir_path(name, kind):
  if kind not in _VM_DIR_SUFFIXES:
    raise ValueError('Invalid kind: {}'.format(repr(kind)))
  return os.path.join(get_data_dir(), name + '_' + kind)

def get_per_vm_data_dir(name = None):
  path = _get_per_vm_dir_path(name, 'data')
  os.makedirs(path, exist_ok = True)
  return path

def _get_installed_vmdef_dir():
  return os.path.join(get_data_dir(), 'installed_vmdefs')

def load_state():
  try:
    with open(os.path.join(get_data_dir(), 'state.json'), 'r') as f:
      return json.load(f)
  except FileNotFoundError:
    return {}

def write_state(state):
  for _ in range(2):
    try:
      with open(os.path.join(get_data_dir(), 'state.json'), 'w') as f:
        json.dump(state, f)
      return
    except FileNotFoundError:
      os.makedirs(get_data_dir())

def _make_rt_name(vm_name, _global, internal, sock):
  name = json.dumps((
    getpass.getuser(), vm_name, _global, internal, sock
  )).encode()
  name = 'virtuator-' + hashlib.sha256(name).hexdigest()
  if is_unix:
    rt = _C('runtime_dir')
    if not rt:
      rt = os.environ.get('XDG_RUNTIME_DIR')
    if not rt:
      import tempfile
      rt = tempfile.gettempdir()
    return os.path.join(rt, name)
  return name + '-mutex'

def acquire_lock(vm_name = None,
                 _global = True,
                 status = None,
                 internal = True):
  name = _make_rt_name(vm_name, _global, internal, False)
  if vm_name and status:
    with _private['lock']:
      vm = _private.setdefault('vms', {}).setdefault(vm_name, {})
      old_status = vm.get('status')
      if old_status:
        e = 'Invalid request: can\'t be {} while already {} for {}'
        raise RuntimeError(e.format(old_status, status, vm_name))
      vm['status'] = status
  if is_unix:
    while True:
      with _private['lock']:
        lock = _private.setdefault('locks', {}).get((vm_name, _global))
        if not lock:
          lock = {'inproc': threading.Lock()}
          _private['locks'][(vm_name, _global, internal)] = lock
      lock['inproc'].acquire()
      if lock is _private['locks'].get((vm_name, _global, internal)):
        break
    while True:
      fh = open(name, 'w')
      fcntl.lockf(fh, fcntl.LOCK_EX)
      if os.fstat(fh.fileno()) == os.stat(name):
        break
      fh.close()
    lock['crossproc'] = fh
  else:
    handle = ctypes.windll.kernel32.CreateMutexW(0, False, name.encode())
    if not handle:
      raise ctypes.WinError()
    r =  ctypes.windll.kernel32.WaitForSingleObject(handle, INFINITE)
    if r == WAIT_FAILED:
      raise ctypes.WinError()
    with _private['lock']:
      _private.setdefault('locks', {})[(vm_name, _global)] = {'handle': handle}

def release_lock(vm_name = None, _global = True, internal = True):
  name = _make_rt_name(vm_name, _global, internal, False)
  lock = _private.get('locks').pop((vm_name, _global, internal))
  if is_unix:
    os.remove(lock['crossproc'].name)
    lock['crossproc'].close()
    lock['inproc'].release()
  else:
    if not ctypes.windll.kernel32.ReleaseMutex(lock['handle']):
      raise ctypes.WinError()
    if not ctypes.windll.kernel32.CloseHandle(lock['handle']):
      raise ctypes.WinError()
  if vm_name:
    with _private['lock']:
      try:
        _private['vms'][vm_name].pop('status')
      except KeyError:
        pass
      if len(_private['locks']) < 1:
        _private.pop('locks')

class _Lock(object):
  def __init__(self, **kwargs):
    self.kwargs = kwargs

  def __enter__(self):
    acquire_lock(**self.kwargs)

  def __exit__(self, exc_type, exc_value, exc_traceback):
    release_lock(**self.kwargs)

class Lock(_Lock):
  def __init__(self, name):
    self.kwargs = {'vm_name': name, '_global': False, 'internal': False}

def get_verbosity():
  verbosity = _private.get('verbosity')
  if verbosity is None:
    verbosity = _C('default_verbosity')
  return verbosity

def _should_vprint(v):
  if type(v) is not int:
    raise ValueError('Invalid verbosity: {}'.format(v))
  return v <= get_verbosity()

def _vprint_prefix(name = None):
  prefix = 'Virtuator > '
  if name:
    prefix += name + ' > '
  return prefix

def vprint(verbosity, *msg, end = '\n', width = None, name = None):
  if not _should_vprint(verbosity):
    return
  msg = ' '.join(map(str, msg))
  if not width:
    width, _ = shutil.get_terminal_size()
  prefix = _vprint_prefix(name = name)
  ewidth = width - len(prefix)
  buf = []
  for line in msg.splitlines():
    buf += [line[i:i+ewidth] for i in range(0, len(line), ewidth)] or ['']
  print('\n'.join(((prefix + i) for i in buf)), end = end)

_CUSTOM_PARAMETER_PREFIX = 'VIRTUATOR_CUSTOM_PARAMETER_'

def make_custom_parameter_error(param,
                                error_message = None,
                                name = None):
  if not error_message:
    error_message = 'Custom parameter {} not found'.format(repr(param))
  d = 'It must be set using one of these methods:\n'
  k = _CUSTOM_PARAMETER_PREFIX + str(param).upper()
  d += '  - An environment variable called {}\n'.format(repr(k))
  k = str(param)
  d += '  - A command line argument e.g. --define {} <value>\n'.format(repr(k))
  d += '  - As a key-value pair in a dictionary passed via the '
  d += 'custom_parameters keyword argument'
  return ValueError('{}: {}'.format(error_message, d))

def get_custom_parameter(param,
                         check = False,
                         default = None,
                         error_message = None,
                         name = None,
                         custom_parameters = None):
  for pk in (param, str(param), str(param).lower(), str(param).upper()):
    try:
      return custom_parameters[pk]
    except KeyError:
      pass
  params = {}
  for k,v in custom_parameters.items():
    if k.upper() == pk:
      return v
  pk = _CUSTOM_PARAMETER_PREFIX + pk
  for i in (pk, pk.lower()):
    try:
      return os.environ[param]
    except KeyError:
      pass
  for k,v in os.environ.items():
    if k.upper() == pk:
      return v
  if check or error_message:
    raise make_custom_parameter_error(param, error_message = error_message)
  return default

def wrap_with_custom_parameters(new_params,
                                name = None,
                                custom_parameters = None):
  merged_params = (custom_parameters if custom_parameters else {}) | new_params
  return VMProxy(name, custom_parameters = merged_params)

def hash_file(path, algorithm):
  h = getattr(hashlib, algorithm.lower())()
  bufsize = _C('disk_buffer_size')
  with open(path, 'rb') as f:
    while True:
      buf = f.read(bufsize)
      if not buf:
        break
      h.update(buf)
  return h.hexdigest()

def download_and_hash(url, path, algorithm, mode = 'xb'):
  h = getattr(hashlib, algorithm.lower())()
  bufsize = _C('socket_buffer_size')
  import urllib.request
  with open(path, mode) as of:
    req = urllib.request.Request(url)
    resp = urllib.request.urlopen(req)
    while True:
      buf = resp.read(bufsize)
      if not buf:
        return h.hexdigest(), of.tell()
      h.update(buf)
      of.write(buf)

# TODO make loop and add more units
def human_size(size):
  g = size / (1024**3)
  if g >= 1:
    return '{:.2f} GB'.format(g)
  m = size / (1024**2)
  if m >= 1:
    return '{:.2f} MB'.format(m)
  k = size / 1024
  if k >= 1:
    return '{:.2f} KB'.format(k)
  return '{} B'.format(size)

def download_entry(path, entry):
  url = entry.get('url')
  vprint(1, 'Downloading {}...'.format(url))
  os.makedirs(os.path.dirname(path), exist_ok = True)
  _hash, size = download_and_hash(url, path, 'sha256', mode = 'wb')
  if size != entry['size']:
    raise RuntimeError('unexpected size')
  if _hash != entry['sha256']:
    raise RuntimeError('unexpected hash')
  return path

def _get_sidecar_vmdef(name = '_virtuator_internal_sidecar'):
  vmdef = _vmdef_from_well_knowns(name, False)
  if not vmdef:
    raise VmdefNotFoundError(name)
  return vmdef

def build_entry(path, name, entry):
  sidecar = _get_sidecar_vmdef()
  return sidecar.build_entry(path, name, entry)

def download_or_build_dependency(name, manifest):
  _apply_packager_overwrites()
  extension = manifest[name + '_extension']
  target_path = os.path.join(get_data_dir(), name+'.'+extension)
  existing = False
  url = manifest.get(name + '_url')
  sha256 = manifest.get(name + '_sha256')
  size = manifest.get(name + '_size')
  if sha256:
    try:
      missing = (hash_file(target_path, 'sha256') != entry['sha256'])
      missing = missing or (os.path.getsize(target_path) != size)
      existing = True
    except FileNotFoundError:
      missing = True
  else:
    missing = os.path.exists(target_path)
  if not missing:
    return target_path
  buildable = manifest.get(name + '_buildable')
  if not url and not buildable:
    raise RuntimeError('missing manifest', name)
  if buildable and _private.get('always_build'):
    return build_entry(target_path, name, entry)
  if url and _private.get('always_download'):
    return download_entry(target_path, entry)
  print('You are missing the dependency {}.\n'.format(repr(name)))
  if url and buildable:
    print('Virtuator can build it for you or it can download a prebuilt copy for you.')
  elif url:
    print('Virtuator can download it for you.')
  elif buildable:
    print('Virtuator can build it for you.')
  print('')
  if existing:
    print('A copy of the file was found but it did not match the expected version.')
    print('A prior download may have been interrupted or the file may be out of date.\n')
  print('Name:', name)
  print('Path:', target_path)
  if url:
    print('URL:', url)
    print('SHA256:', sha256)
    print('Size:', human_size(size))
  print('')
  print('Use the --always-build option to automatically build dependencies in the future.')
  print('Use the --always-download option to automatically download dependencies in the future.')
  print('If a dependency is both buildable and downloadable, building will take priority.')
  print('')
  print('What would you like to do?')
  if url:
    print('D) Download')
  if buildable:
    print('B) Build')
  print('Q) Quit')
  print('')
  inp = input('> ').lower()
  if inp == 'd' and url:
    return download_entry(target_path, entry)
  if inp == 'b' and buildable:
    return build_entry(target_path, name, entry)
  print('Quitting...')
  raise FileNotFoundError(target_path)

def create_rpc_server():
  kwargs = {
    'stdout': subprocess.DEVNULL,
    'stderr':subprocess.DEVNULL,
    'env': dict(os.environ) | {'VIRTUATOR_DAEMONIZED': '1'},
  }
  detached_process = getattr(subprocess, 'DETACHED_PROCESS', None)
  if detached_process is None:
    kwargs['start_new_session'] = True
    try:
      os.unlink(get_socket_path())
    except FileNotFoundError:
      pass
  else:
    kwargs['creationflags'] = detached_process
  subprocess.Popen((sys.executable, __file__), **kwargs)

def is_daemon():
  return bool(os.environ.get('VIRTUATOR_DAEMONIZED'))

def serialize_exception(exc):
  return {
    'class': exc.__class__.__name__,
    'args': exc.args,
  }

def deserialize_exception(exc):
  eclass = globals().get(exc.get('class'))
  if hasattr(builtins, exc.get('class')):
    eclass = getattr(builtins, exc.get('class'))
  try:
    valid_class = issubclass(eclass, Exception)
  except TypeError:
    valid_class = False
  args = exc.get('args')
  if valid_class and type(args) is list:
    return eclass(*args)
  return RpcUndeserializableError(exc)

def find_rpc_receiver(class_name):
  HWND_MESSAGE = ctypes.wintypes.HWND(-3)
  hwnd = ctypes.windll.user32.FindWindowExW(HWND_MESSAGE, 0, class_name, 0)
  return hwnd if hwnd else None

def get_socket_path():
  p = _C('socket_path')
  if p:
    return p
  return _make_rt_name(None, True, True, True)

def get_rpc_server():
  if is_unix:
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
      server.connect(get_socket_path())
    except (FileNotFoundError, ConnectionRefusedError):
      return None
  else:
    return find_rpc_receiver(RPC_SERVER_CLASS)
  return server

def get_or_create_rpc_server():
  acquire_lock()
  try:
    server = get_rpc_server()
    if not server:
      create_rpc_server()
    start = time.time()
    while (time.time() - start) < _C('rpc_server_startup_timeout'):
      server = get_rpc_server()
      if server:
        return server
      time.sleep(_C('rpc_server_startup_polling_delay'))
  finally:
    release_lock()
  raise RpcTimoutError('Server did not start')

def try_parse_payload(buffer):
  try:
    payload = json.loads(buffer)
    if type(payload) is not dict:
      return None
    if not payload.get('action'):
      return None
    if not is_unix and not payload.get('id'):
      return None
    return payload
  except (json.JSONDecodeError, ValueError):
    return None

def _cleanup_rpc_receiver_with_lock():
  hwnd = _private.pop('rpc_receiver_hwnd')
  if not ctypes.windll.user32.DestroyWindow(hwnd):
    raise ctypes.WinError()
  class_name = _private.pop('rpc_receiver_class')
  if not ctypes.windll.user32.UnregisterClassW(class_name, 0):
    raise ctypes.WinError()
  _private.pop('rpc_window_proc')
  _private.pop('rpc_receiver_thread')
  try:
    _private.pop('rpc_receiver_done')
  except KeyError:
    pass

def _wndproc(hwnd, msg, wparam, lparam):
  if msg == WM_COPYDATA:
    lcdata = ctypes.cast(lparam, ctypes.POINTER(COPYDATASTRUCT))
    cdata = lcdata.contents
    if cdata.dwData == RPC_MAGIC:
      _private['rpc_received_callback'](cdata.lpData)
      return 1
  return ctypes.windll.user32.DefWindowProcW(hwnd, msg, wparam, lparam)

def _register_window_class(name):
  WNDPROC = ctypes.WINFUNCTYPE(
    ctypes.c_int,
    ctypes.wintypes.HWND,
    ctypes.c_uint,
    ctypes.wintypes.WPARAM,
    ctypes.wintypes.LPARAM
  )
  class WNDCLASSW(ctypes.Structure):
    _fields_ = [
      ('style', ctypes.wintypes.UINT),
      ('lpfnWndProc', WNDPROC),
      ('cbClsExtra', ctypes.c_int),
      ('cbWndExtra', ctypes.c_int),
      ('hInstance', ctypes.wintypes.HANDLE),
      ('hIcon', ctypes.wintypes.HANDLE),
      ('hCursor', ctypes.wintypes.HANDLE),
      ('hbrBackground', ctypes.wintypes.HANDLE),
      ('lpszMenuName', ctypes.wintypes.LPCWSTR),
      ('lpszClassName', ctypes.wintypes.LPCWSTR),
    ]
  ctypes.windll.user32.DefWindowProcW.argtypes = [
    ctypes.wintypes.HWND,
    ctypes.c_uint,
    ctypes.wintypes.WPARAM,
    ctypes.wintypes.LPARAM,
  ]
  cls = WNDCLASSW()
  cls.style = 0
  window_proc = WNDPROC(_wndproc)
  _private['rpc_window_proc'] = window_proc
  cls.lpfnWndProc = window_proc
  cls.cbClsExtra = 0
  cls.cbWndExtra = 0
  cls.hInstance = 0
  cls.hIcon = 0
  cls.hCursor = 0
  cls.hbrBackground = 0
  cls.lpszMenuName = 0
  cls.lpszClassName = name
  atom = ctypes.windll.user32.RegisterClassW(ctypes.byref(cls))
  if atom == 0:
    raise ctypes.WinError()
  return atom

# TODO move rpc receiver to own daemon thread to prevent blocking main thread
def create_rpc_receiver(client_pid = None):
  if client_pid is None:
    class_name = RPC_SERVER_CLASS
  else:
    class_name = RPC_CLIENT_CLASS_PREFIX + str(client_pid)
  atom = _register_window_class(class_name)
  _private['rpc_receiver_class'] = class_name
  HWND_MESSAGE = ctypes.wintypes.HWND(-3)
  hwnd = ctypes.windll.user32.CreateWindowExW(
    0,
    class_name,
    class_name,
    0,
    0,
    0,
    0,
    0,
    HWND_MESSAGE,
    0,
    0,
    0
  )
  if not hwnd:
    raise ctypes.WinError()
  _private['rpc_receiver_hwnd'] = hwnd
  _private['rpc_receiver_thread'] = threading.get_ident()

def ensure_rpc_result_receiver_ready(client_pid):
  with _private['lock']:
    if 'rpc_receiver_hwnd' not in _private:
      create_rpc_receiver(client_pid = client_pid)

def do_rpc_loop(callback):
  _private['rpc_received_callback'] = callback
  msg = ctypes.wintypes.MSG()
  while True:
    ret = ctypes.windll.user32.GetMessageW(ctypes.byref(msg),
                                           _private['rpc_receiver_hwnd'],
                                           0,
                                           0)
    if ret == 0:
      return
    if ret == -1:
      raise ctypes.WinError()
    ctypes.windll.user32.TranslateMessage(ctypes.byref(msg))
    ctypes.windll.user32.DispatchMessageW(ctypes.byref(msg))

def stop_rpc_loop_from_loop_thread():
  ctypes.windll.user32.PostQuitMessage(0)
  
def stop_rpc_loop_from_external_thread():
  send_rpc_payload(_private['rpc_receiver_hwnd'],
                   None,
                   no_reply = True,
                   msg = WM_QUIT)

if not is_unix:
  class COPYDATASTRUCT(ctypes.Structure):
    _fields_ = [
      ('dwData', ctypes.wintypes.HANDLE),
      ('cbData', ctypes.wintypes.DWORD),
      ('lpData', ctypes.c_char_p),
    ]

def send_rpc_payload(recipient, payload, no_reply = False, msg = WM_COPYDATA):
  pl = json.dumps(payload).encode() + b'\0'
  cdata = COPYDATASTRUCT()
  cdata.dwData = ctypes.wintypes.HANDLE(RPC_MAGIC)
  cdata.cbData = len(pl)
  cdata.lpData = pl
  send = getattr(ctypes.windll.user32,
                 'PostMessageW' if no_reply else 'SendMessageW')
  ret = send(recipient,
             msg,
             _private['rpc_receiver_hwnd'],
             ctypes.byref(cdata))
  if not ret:
    raise ctypes.WinError()

def socket_readline(socket):
  payload = b''
  setblocking = getattr(socket, 'setblocking', None)
  bufsize = _C('socket_buffer_size') if setblocking else 1
  blocking = True
  while True:
    buf = socket.recv(1 if blocking else bufsize)
    if not buf and blocking:
      return payload
    payload += buf
    if buf.endswith(b'\n'):
      return payload
    if buf and blocking and setblocking:
      blocking = False
      setblocking(False)
    elif not buf and not blocking and setblocking:
      blocking = True
      setblocking(True)

def _get_or_maybe_create_rpc_server(payload, can_start_daemon):
  if can_start_daemon:
    return get_or_create_rpc_server()
  server = get_rpc_server()
  name = payload.get('name')
  if not server:
    if name:
      raise VmNotFoundError(name)
    else:
      raise RpcTimoutError('Server not running')
  return server

def _rpc_response_callback(payload):
  payload = json.loads(payload)
  result = _private['rpc_results'].get(payload.get('id'))
  if result is None:
    return
  result['payload'] = payload
  ready = result.get('ready')
  if ready is None:
    stop_rpc_loop_from_loop_thread()
  else:
    ready.set()

def _do_one_rpc_call(payload, can_start_daemon = False):
  if is_unix:
    server = _get_or_maybe_create_rpc_server(payload, can_start_daemon)
    server.send((json.dumps(payload)+'\n').encode())
    rpayload = json.loads(socket_readline(server))
  else:
    _id = generate_id()
    pid = os.getpid()
    payload['id'] = _id
    payload['pid'] = pid
    ensure_rpc_result_receiver_ready(pid)
    server = _get_or_maybe_create_rpc_server(payload, can_start_daemon)
    result = {}
    with _private['lock']:
      results = _private.get('rpc_results')
      is_watcher = not results
      if not is_watcher:
        result['ready'] = threading.Event()
      if results is None:
        results = {_id: result}
        _private['rpc_results'] = results
      else:
        results[_id] = result
    send_rpc_payload(server, payload)
    if is_watcher:
      do_rpc_loop(_rpc_response_callback)
      rpayload = result['payload']
    else:
      try:
        result['ready'].wait()
      except KeyError:
        pass
      rpayload = result.get('payload')
      if not rpayload:
        do_rpc_loop(_rpc_response_callback)
        rpayload = result['payload']
    done_event = None
    with _private['lock']:
      if len(results) == 1:
        _private.pop('rpc_results')
        if threading.get_ident() == _private.get('rpc_receiver_thread'):
          _cleanup_rpc_receiver_with_lock()
        else:
          devent = _private.get('rpc_receiver_done')
          if devent:
            devent.set()
      else:
        results.pop(_id)
        if threading.get_ident() == _private.get('rpc_receiver_thread'):
          done_event = threading.Event()
          _private['rpc_receiver_done'] = done_event
        for next_watcher in results.values():
          if not next_watcher.get('payload'):
            next_watcher.pop('ready').set()
            break
    while done_event:
      done_event.wait()
      done_event.clear()
      with _private['lock']:
        if len(_private.get('rpc_results', {})) < 2:
          _cleanup_rpc_receiver_with_lock()
          break
  error = rpayload.get('error')
  if error:
    raise deserialize_exception(error)
  return rpayload['result']

# TODO need to make sure windows can interrupt w/ ctrl + c
def _rpc_call(payload, can_start_daemon = False, retry_if_interrupted = True):
  for k, v in getattr(payload, 'items', tuple)():
    if type(v) is types.MappingProxyType:
      payload[k] = dict(v)
  while True:
    try:
      return _do_one_rpc_call(payload, can_start_daemon = can_start_daemon)
    except RpcInterruptedError:
      if not retry_if_interrupted:
        raise
      time.sleep(_C('rpc_server_startup_polling_delay'))

def validate_vmdef(vmdef, require_build = True):
  vmdef_dict = vmdef if type(vmdef) is dict else vmdef.__dict__
  name = vmdef_dict.get('NAME', None)
  validate_name(name)
  ver = vmdef_dict.get('VMDEF_VERSION', None)
  if type(ver) in (int, float) and ver > 1:
    raise ValueError('VMDEF_VERSION too high - Virtuator may need an update')
  if ver != 1:
    raise ValueError('VMDEF_VERSION missing or invalid')
  build = vmdef_dict.get('BUILD', None)
  if require_build and type(build) is not type(_C):
    raise TypeError('BUILD was not a function')
  if vmdef_dict.get('SYSTEMIMG', None) not in (None, DEFAULT_SYSTEMIMG):
    raise ValueError('SYSTEMIMG was invalid - Virtuator may need an update')

def try_load_vmdef_code(name,
                        code,
                        location = None,
                        require_build = True,
                        cache = True):
  validate_name(name)
  if not location:
    vmdef = types.ModuleType(name)
    location = '<vmdef:{}>'.format(name)
  else:
    import importlib, importlib.machinery
    loader = importlib.machinery.SourceFileLoader(name, location)
    spec = importlib.util.spec_from_file_location(name,
                                                  location = location,
                                                  loader = loader)
    vmdef = importlib.util.module_from_spec(spec)
  comp = compile(code, location, 'exec')
  exec(comp, vmdef.__dict__)
  try:
    validate_vmdef(vmdef, require_build = require_build)
  except (TypeError, ValueError):
    return None
  if cache:
    _private['vmdefs'][vmdef.NAME] = vmdef
  return vmdef

def try_load_vmdef_path(name, path, require_build = True, cache = True):
  try:
    with open(path, 'r') as f:
      code = f.read()
  except FileNotFoundError:
    return None
  return try_load_vmdef_code(name,
                             code,
                             location = path,
                             require_build = require_build,
                             cache = cache)

def vmdef_from_memory(name):
  with _private['lock']:
    return _private.setdefault('vmdefs', {}).get(name)

def _vmdef_from_well_knowns(name, require_build):
  lib_dir = os.path.dirname(os.path.realpath(__file__))
  fname = name + '.vmdef'
  roots = [lib_dir, get_data_dir()]
  if PACKAGER_OVERWRITABLE_WELL_KNOWN_VMDEF_DIR:
    roots.insert(0, PACKAGER_OVERWRITABLE_WELL_KNOWN_VMDEF_DIR)
  for root in roots:
    path = os.path.join(root, 'well_known_vmdefs', fname)
    vmdef = try_load_vmdef_path(name, path, require_build = require_build)
    if vmdef:
      return vmdef
  try:
    tar_path = download_or_build_dependency('well_known_vmdefs',
                                            PACKAGER_OVERWRITABLE_DOWNLOAD_MANIFEST)
  except (KeyError, FileNotFoundError):
    return None
  import tarfile
  with tarfile.open(tar_path) as tf:
    try:
      code = tf.extractfile(fname).read()
    except KeyError:
      code = None
    if code:
      vmdef = try_load_vmdef_code(name,
                                  code,
                                  location = os.path.join(tar_path, fname))
      if vmdef:
        return vmdef
  return None

def _vmdef_from_user_provided_vmdefs(name, state, require_build):
  path = state.setdefault('vmdefs', {}).get(name)
  if path:
    vmdef = try_load_vmdef_path(name, path, require_build = require_build)
    if vmdef:
      return vmdef
  for d in [_get_installed_vmdef_dir()] + _C('vmdef_search_paths'):
    vmdef = try_load_vmdef_path(name,
                                os.path.join(d, name + '.vmdef'),
                                require_build = require_build)
    if vmdef and vmdef.NAME == name:
      return vmdef
  return None

def vmdef_from_disk(name, state, fallback_to_vm_name, require_build = True):
  if name is None:
    return None
  prefer_wk = not _C('prefer_user_provided_vmdefs_over_well_known_vmdefs')
  if prefer_wk:
    vmdef = _vmdef_from_well_knowns(name, require_build)
    if vmdef:
      return vmdef
  if state is None:
    acquire_lock()
    state = load_state()
    release_lock()
  vmdef = _vmdef_from_user_provided_vmdefs(name, state, require_build)
  if vmdef:
    return vmdef
  if not prefer_wk:
    vmdef = _vmdef_from_well_knowns(name, require_build)
    if vmdef:
      return vmdef
  if fallback_to_vm_name:
    vm = state.setdefault('vms', {}).get(name)
    if vm:
      vmdef = vmdef_from(vm.get('vmdef_name'),
                         fallback_to_vm_name = False,
                         require_build = require_build)
      if vmdef:
        return vmdef
  return None

def vmdef_from(vmdef_name,
               state = None,
               fallback_to_vm_name = True,
               require_build = True,
               name = None):
  vmdef = vmdef_from_memory(vmdef_name)
  if vmdef:
    return vmdef
  return vmdef_from_disk(vmdef_name,
                         state,
                         fallback_to_vm_name,
                         require_build = require_build)

def vmdef_exists(name):
  vmdef = vmdef_from(name, fallback_to_vm_name = False)
  return bool(vmdef)

def make_disk_path(name, idx, clean = False):
  sfx = '_clean' if clean else ''
  return os.path.join(get_data_dir(), '{}_disk{}{}.img'
                                .format(name, int(idx), sfx))

def _disk_from(src_name,
               src_disk,
               dst_disk = None,
               can_rebuild_src = False,
               has_vm_lock = False,
               name = None,
               custom_parameters = None):
  clean_disk = make_disk_path(src_name, src_disk, clean = True)
  if dst_disk is None and os.path.isfile(clean_disk):
    return clean_disk
  original_disk = make_disk_path(src_name, src_disk, clean = False)
  if not has_vm_lock:
    acquire_lock(vm_name = src_name, _global = False, status = 'copying disks')
  acquire_lock()
  try:
    try:
      state = load_state()
      src_vm = state.setdefault('vms', {}).get(src_name)
      if dst_disk is not None:
        dst_vm = state.get('vms').setdefault(name, {})
        dst_disks = dst_vm.setdefault('disks', [])
        if dst_disk > len(dst_disks):
          IndexError('destination index too high')
        dest = make_disk_path(name, dst_disk)
        try:
          shutil.copy(clean_disk, dest)
        except FileNotFoundError:
          pass
      if src_vm.get('clean') and original_disk in src_vm.get('disks', []):
        if dst_disk is not None:
          shutil.copy(original_disk, dest)
          if dst_disk == len(dst_disks):
            dst_disks.append(dest)
            write_state(state)
          return original_disk
        return original_disk if os.path.isfile(original_disk) else None
    finally:
      release_lock()
    if dst_disk is not None and can_rebuild_src:
      _rm(src_name, state, custom_parameters)
      _build(name = src_name,
             custom_parameters = custom_parameters,
             has_vm_lock = True)
      return _disk_from(src_name,
                        src_disk,
                        dst_disk = dst_disk,
                        can_rebuild_src = False,
                        has_vm_lock = True,
                        name = name,
                        custom_parameters = custom_parameters)
  finally:
    if not has_vm_lock:
      release_lock(vm_name = src_name, _global = False)
  return None

def disk_from(src_name,
              src_disk,
              dst_disk,
              can_rebuild_src = False,
              name = None,
              custom_parameters = None):
  if dst_disk is None:
    raise ValueError('dst_disk cannot be None')
  return _disk_from(src_name,
                    src_disk,
                    dst_disk = dst_disk,
                    can_rebuild_src = can_rebuild_src,
                    name = name,
                    custom_parameters = custom_parameters)

def try_find_clean_disk(src_name,
                        src_disk,
                        name = None,
                        custom_parameters = None):
  return _disk_from(src_name,
                    src_disk,
                    dst_disk = None,
                    name = name,
                    custom_parameters = custom_parameters)

@functools.cache
def _which(cmd):
  path = list(filter(bool, os.environ.get('PATH', '').split(os.pathsep)))
  program_files = os.environ.get('PROGRAMFILES')
  if program_files:
    path.append(os.path.join(program_files, 'qemu'))
  return shutil.which(cmd, path = os.pathsep.join(path))

def get_machine(name = None):
  machine = platform.machine()
  _apply_packager_overwrites()
  return NORMALIZED_ARCHS.get(machine, machine)

def _get_arch_and_backend_cmd(compatible = None, vmdef = None):
  if vmdef:
    compatible = getattr(vmdef, 'ARCH', None)
  machine = get_machine()
  matches = []
  for backend in PACKAGER_OVERWRITABLE_BACKENDS:
    for arch, cmds in backend.items():
      if compatible and NORMALIZED_ARCHS.get(arch, arch) not in compatible:
        continue
      for cmd in cmds:
        if arch == machine:
          matches.insert(0, (arch, cmd))
        else:
          matches.append((arch, cmd))
  for arch, cmd in matches:
    c = _which(cmd)
    if c:
      return arch, c
  if vmdef:
    e = '{} requires {} but no matching backends were available'
    e = e.format(repr(vmdef.NAME), repr(compatible))
  else:
    e = 'Required {} but no matching backends were available'
    e = e.format(repr(compatible))
  raise IncompatibleBackendsError(e)

def get_arch(name = None):
  return _get_arch_and_backend_cmd(vmdef = vmdef_from(name))[0]

def get_backend_cmd(arch):
  compat = (arch,) if type(arch) is str else arch
  return _get_arch_and_backend_cmd(compatible = compat)[1]

def _get_backend(arch = None, vmdef = None):
  if arch:
    cmd = get_backend_cmd(arch)
  else:
    cmd = _get_arch_and_backend_cmd(vmdef = vmdef)[1]
  bin_name = os.path.basename(cmd)
  if cmd and bin_name.startswith('qemu-'):
    return 'qemu'
  return None

def get_backend(name = None):
  return _get_backend(vmdef = vmdef_from(name))

def get_systemimg(name):
  vmdef = vmdef_from(name)
  if not vmdef:
    raise VmdefNotFoundError(name)
  return getattr(vmdef, 'SYSTEMIMG', None)

def get_systemimg_path(name, check = True):
  vmdef = vmdef_from(name)
  if not vmdef:
    raise VmdefNotFoundError(name)
  k = getattr(vmdef, 'SYSTEMIMG', None)
  if not k:
    k = DEFAULT_SYSTEMIMG
  k += '_' + _get_arch_and_backend_cmd(vmdef = vmdef)[0]
  p = os.path.join(get_data_dir(), k + '.iso')
  if check:
    download_or_build_dependency(k, PACKAGER_OVERWRITABLE_SYSTEMIMG_MANIFEST)
  return p

def _is_non_empty_file(path):
  if not path:
    return False
  try:
    s = os.stat(path)
  except FileNotFoundError:
    return False
  if not stat.S_ISREG(s.st_mode):
    return False
  return s.st_size > 0

def _ensure_vm_metadata_loaded(kwargs, vm):
  if vm:
    return vm
  acquire_lock()
  state = load_state()
  release_lock()
  name = kwargs.get('name')
  vm = state.get('vms', {}).get(name)
  if not vm:
    raise VmNotFoundError(name)
  return vm

def populate_default_boot_kwargs(kwargs, vm = None, vmdef = None):
  if 'disks' not in kwargs:
    vm = _ensure_vm_metadata_loaded(kwargs, vm)
    disks = vm.get('disks')
    if disks:
      kwargs['disks'] = disks
  if vmdef is None:
    vm = _ensure_vm_metadata_loaded(kwargs, vm)
    vmdef_name = vm.get('vmdef_name') or kwargs.get('name')
    vmdef = vmdef_from(vmdef_name)
  disks = []
  disk_defs = getattr(vmdef, 'DISKS', [])
  for idx, disk in enumerate(kwargs.get('disks', [])):
    if type(disk) is str:
      disk = {'PATH': disk}
    try:
      disk_def = disk_defs[idx]
      disk_def_type = disk_def.get('TYPE')
      if disk_def_type and 'TYPE' not in disk:
        disk['TYPE'] = disk_def_type
    except IndexError:
      pass
    disks.append(disk)
  kwargs['disks'] = disks
  if 'memory' not in kwargs:
    kwargs['memory'] = getattr(vmdef, 'MEMORY', None)
  if 'network' not in kwargs:
    kwargs['network'] = getattr(vmdef, 'NETWORK', None)
  if 'ports' not in kwargs:
    kwargs['ports'] = _private.get('ports', [])
  if 'graphics' not in kwargs:
    kwargs['graphics'] = (getattr(vmdef, 'GRAPHICS', False) or
                          _private.get('force_graphics'))
  if 'uefi' not in kwargs:
    kwargs['uefi'] = getattr(vmdef, 'UEFI', True)
  if 'secure_boot' not in kwargs:
    kwargs['secure_boot'] = getattr(vmdef, 'SECURE_BOOT', False)
  if 'tpm' not in kwargs:
    tpm = getattr(vmdef, 'TPM', None)
    tpm_version = None
    if tpm and type(tpm) is type(_C):
      params = kwargs.get('custom_parameters')
      kwargs['tpm'] = tpm(VMProxy(kwargs.get('name',
                                             custom_parameters = params)))
    elif type(tpm) in (int, float, str, bytes):
      tpm_version = float(tpm)
    elif tpm is True:
      tpm_version = True
    if tpm_version:
      kwargs['tpm'] = start_tpm(tpm_version, name = kwargs['name'])
  arch = kwargs.get('arch')
  if not arch:
    arch = _get_arch_and_backend_cmd(vmdef = vmdef)[0]
    kwargs['arch'] = arch
  if 'firmware' not in kwargs:
    sboot = bool(kwargs.get('secure_boot'))
    for kind in ('uefi', 'bios'):
      try:
        if kwargs.pop(kind):
          kwargs['firmware'] = _get_firmware(arch, kind == 'uefi', sboot, False)
          if 'nvram_template' not in kwargs:
            kwargs['nvram_template'] = _get_firmware(arch, kind == 'uefi', sboot, True)
          break
      except KeyError:
        pass

def _get_vmdef_for_vm(name, state):
  if state is None:
    acquire_lock()
    state = load_state()
    release_lock()
  vmdef_name = state.get('vms', {}).get(name, {}).get('vmdef_name')
  if not vmdef_name:
    vmdef_name = name
  return vmdef_from(vmdef_name, state = state)

def force_stop(name = None):
  return _rpc_call({'action': 'stop_vm', 'name': name})

def _stop(name = None, state = None, custom_parameters = None):
  vmdef = _get_vmdef_for_vm(name, state)
  stop_func = getattr(vmdef, 'STOP', None)
  if stop_func and not _private.get('force_command'):
    _start_catching_infinite_recursion(name, 'stop')
    try:
      stop_func(VMProxy(name, custom_parameters = custom_parameters))
    finally:
      _finish_catching_infinite_recursion(name, 'stop')
  try:
    force_stop(name = name)
  except VmNotFoundError as exc:
    if not stop_func:
      raise exc

def stop(name = None, custom_parameters = None):
  if name:
    acquire_lock(vm_name = name, _global = False, status = 'stopping')
    try:
      _stop(name, custom_parameters = custom_parameters)
    finally:
      release_lock(vm_name = name, _global = False)

def _rm(name, state, custom_parameters):
  if name in list_running_vms():
    try:
      _stop(name, state)
    except VmNotFoundError:
      pass
  vmdef = _get_vmdef_for_vm(name, state)
  rm_func = getattr(vmdef, 'RM', None)
  if rm_func:
    _start_catching_infinite_recursion(name, 'rm')
    try:
      rm_func(VMProxy(name, custom_parameters = custom_parameters))
    finally:
      _finish_catching_infinite_recursion(name, 'rm')
  acquire_lock()
  state = load_state()
  try:
    vm = state['vms'].pop(name)
    for disk_path in vm.get('disks', []):
      try:
        os.remove(disk_path)
      except FileNotFoundError:
        pass
    for kind in _VM_DIR_SUFFIXES:
      try:
        shutil.rmtree(_get_per_vm_dir_path(name, kind))
      except FileNotFoundError:
        pass
    explicitly_registered = set(state.get('explicitly_registered_vmdefs', []))
    for v in (vm.get('vmdef_name'), name):
      if (not any((i.get('vmdef_name') == v for i in state['vms'].values()))
          and v not in explicitly_registered):
        try:
          state['vmdefs'].pop(v)
        except KeyError:
          pass
    write_state(state)
  except KeyError:
    pass
  release_lock()

def rm(name = None, custom_parameters = None):
  if name:
    acquire_lock(vm_name = name, _global = False, status = 'removing')
    try:
      _rm(name, None, custom_parameters)
    finally:
      release_lock(vm_name = name, _global = False)

def _start_catching_infinite_recursion(name, kind):
  k = '{}ing_vms'.format(kind)
  with _private['lock']:
    vms = _private.get(k)
    if vms is None:
      vms = set()
      _private[k] = vms
    if name in vms:
      e = (
        'Infinite recursion detected for VM {}: cannot call {}() from the {}() function'
        .format(repr(name), kind, kind.upper())
      )
      raise RuntimeError(e)
    vms.add(name)

def _finish_catching_infinite_recursion(name, kind):
  k = '{}ing_vms'.format(kind)
  with _private['lock']:
    vms = _private[k]
    vms.remove(name)
    if not vms:
      _private.pop(k)

def _build(name = None,
           vmdef_name = None,
           exclusive = False,
           has_vm_lock = False,
           custom_parameters = None):
  if not has_vm_lock:
    acquire_lock(vm_name = name, _global = False, status = 'building')
  try:
    if vmdef_name is None:
      vmdef_name = name
    if name is None:
      raise TypeError('VM name not specified')
    validate_name(name)
    vmdef = vmdef_from_memory(vmdef_name)
    lock_released = False
    new_vm = False
    acquire_lock()
    state = load_state()
    vm = state.setdefault('vms', {}).get(name)
    if not vmdef:
      release_lock()
      lock_released = True
      vmdef = vmdef_from_disk(vmdef_name, state, False)
      if not vmdef:
        raise VmdefNotFoundError(vmdef_name)
    if vm and exclusive and _private.get('force_command'):
      if not lock_released:
        release_lock()
        lock_released = True
      _rm(name, state, custom_parameters)
      vm = None
    if not vm:
      new_vm = {'disks': []}
      if vmdef_name != name:
        new_vm['vmdef_name'] = vmdef_name
      if lock_released:
        acquire_lock()
        state = load_state()
        vm = state.setdefault('vms', {}).get(name)
        if vm and exclusive and _private.get('force_command'):
          _rm(name, state, custom_parameters)
          vm = None
      if not vm:
        for idx, disk in enumerate(getattr(vmdef, 'DISKS', [])):
          new_vm['disks'].append(make_disk_path(name, idx))
        state['vms'][name] = new_vm
        vmdef_path = getattr(vmdef, '__file__', None)
        if vmdef_path:
          vmdefs = state.get('vmdefs', {})
          old_path = vmdefs.get(vmdef_name)
          if old_path and old_path != vmdef_path:
            raise RuntimeError(
              'VMDEF {} was registered twice with two different paths: {}'
               .format(repr(vmdef_name),
                       '{} and {}'.format(repr(old_path), repr(vmdef_path)))
            )
          if vmdef_name not in list_well_known_vmdefs():
            [vmdef_name] = vmdef_path
          if vmdefs:
            state['vmdefs'] = vmdefs
        write_state(state)
    if not lock_released:
      release_lock()
    if vm and exclusive:
      if not _private.get('force_command'):
        raise VmExistsError('VM {} already exists'.format(repr(name)))
    if new_vm:
      skip_boot = getattr(vmdef, 'SKIP_BUILD_BOOT', False)
      if not skip_boot:
        systemimg = get_systemimg_path(vmdef_name)
      if not getattr(vmdef, 'SKIP_DISK_CREATION', False):
        backend = _get_backend(vmdef = vmdef)
        for idx, disk_path in enumerate(new_vm['disks']):
          disk = vmdef.DISKS[idx]
          if backend == 'qemu':
            qemu_img = _which('qemu-img')
            if not qemu_img:
              raise FileNotFoundError('qemu-img')
            subprocess.check_call((qemu_img, 'create', '-f', 'qcow2',
                                  disk_path, vmdef.DISKS[idx]['SIZE']))
          else:
            e = 'Unsupported backend: {}'.format(repr(backend))
            raise NotImplementedError(e)
          # TODO disk types, validate size, validate type
      if not skip_boot:
        kw = {
          'name': name,
          'uefi': True,
          'prefer_discs': True,
          'network': 'user',
          'custom_parameters': custom_parameters,
        }
        populate_default_boot_kwargs(kw, new_vm, vmdef)
        kw.setdefault('discs', []).insert(0, systemimg)
        _boot(**kw)
        ensure_shell(name = name)
      _start_catching_infinite_recursion(name, 'build')
      try:
        vmdef.BUILD(VMProxy(name, custom_parameters = custom_parameters))
      finally:
        _finish_catching_infinite_recursion(name, 'build')
      if not skip_boot:
        _stop(name)
      acquire_lock()
      state = load_state()
      state['vms'][name]['built'] = True
      state['vms'][name]['clean'] = True
      write_state(state)
      try:
        if _C('cache_clean_copies_of_vms'):
          n = vmdef_name if vmdef_name else name
          for idx, disk_path in enumerate(new_vm.get('disks', [])):
            try:
              shutil.copy(disk_path, make_disk_path(n, idx, clean = True))
            except FileNotFoundError:
              pass
      finally:
        release_lock()
      return new_vm
    return vm
  finally:
    if not has_vm_lock:
      release_lock(vm_name = name, _global = False)

def build(name = None, vmdef_name = None, custom_parameters = None):
  _build(name = name,
         vmdef_name = vmdef_name,
         exclusive = True,
         custom_parameters = custom_parameters)

def _realtime_shell_output_worker(priv):
  stdout = sys.stdout.buffer
  name = priv['name']
  pre = priv.get('pre', '').encode()
  if pre:
    post = priv.get('post', '').encode()
    ret_delim = priv.get('ret_delim', '').encode()
    delims_seen = 0
    max_buf = _C('max_stdio_buffer_size')
  while not priv['exited']:
    buf = read_output(name = name)
    if not buf:
      priv['exited'] = True
    if priv['exited']:
      if buf:
        with _private['lock']:
          vm = _private.setdefault('vms', {}).setdefault(name, {})
          vm['buf'] = vm.get('buf', b'') + buf
    else:
      if pre:
        with _private['lock']:
          vm = _private.setdefault('vms', {}).setdefault(name, {})
          vm_buf = vm.get('buf', b'') + buf
          # TODO prevent flushing partial delim
          buffer_updated = False
          if delims_seen == 0:
            idx = vm_buf.rfind(pre)
            if idx >= 0:
              delims_seen = 1
              buf = vm_buf[idx+len(pre):]
              buffer_updated = True
          if delims_seen == 1:
            idx = vm_buf.rfind(post)
            if idx >= 0:
              delims_seen = 2
              buf = vm_buf[-len(buf):idx]
              buffer_updated = True
          if delims_seen == 2:
            if not buffer_updated:
              buf = b''
            idx = vm_buf.rfind(ret_delim)
            if idx >= 0:
              ret = int(vm_buf[vm_buf.rfind(post)+len(post):idx])
              priv['ret'] = ret
              priv['exited'] = True
              if priv.get('interrupt_on_exit'):
                os.kill(os.getpid(), signal.SIGINT)
              vm_buf = vm_buf[idx+len(ret_delim):]
          vm['buf'] = vm_buf[-max_buf:]
      stdout.write(buf)
      stdout.flush()

def realtime_shell(command = None, name = None, interrupt_on_exit = True):
  priv = {'name': name, 'exited': False, 'interrupt_on_exit': interrupt_on_exit}
  if command:
    pre, post, ret_delim = (generate_id()+generate_id() for _ in range(3))
    ret_var = generate_id()
    i = len(pre)//2
    cmd = 'printf {};printf {};{};'.format(pre[:i], pre[i:], command)
    if not cmd.rstrip().endswith(';'):
      cmd += ';'
    cmd += '{}=$?;printf {};printf {};'.format(ret_var, post[:i], post[i:])
    cmd += 'printf ${};'.format(ret_var)
    cmd += 'printf {};printf {};'.format(ret_delim[:i], ret_delim[i:])
    cmd += 'unset {};\n'.format(ret_var)
    priv['pre'], priv['post'], priv['ret_delim'] = pre, post, ret_delim
  out_thread = threading.Thread(target = _realtime_shell_output_worker,
                                args = (priv,),
                                daemon = True)
  if not command:
    print('Entering realtime shell for {}...'.format(name))
    print('Press Ctrl + C to exit the realtime shell')
    print('')
  out_thread.start()
  stdin = sys.stdin
  if command:
    send_keys(cmd, name = name)
  while not priv['exited']:
    try:
      c = stdin.read(1)  # TODO: fix hang on exit
    except KeyboardInterrupt:
      priv['exited'] = True
    if not priv['exited']:
      send_keys(c, name = name)
  if not command:
    print('\n\nExiting realtime shell for {}...'.format(name))
  return priv.get('ret')

def basic_shell(name = None):
  exit_phrase = _C('shell_exit_phrase')
  rtime_phrase = _C('realtime_shell_phrase')
  print('Opening basic shell for {}'.format(name))
  print('Enter the command {} to leave the shell'.format(repr(exit_phrase)))
  print('Enter the command {} to use the realtime shell'
          .format(repr(rtime_phrase)))
  print('')
  while True:
    try:
      i = input('Virtuator>{}>Shell> '.format(name))
    except KeyboardInterrupt:
      send_keys(CTRL_C, name = name)
      i = ''
    except EOFError:
      print('')
      return
    if i == exit_phrase:
      return
    try:
      if i == rtime_phrase:
        realtime_shell(name = name)
        continue
      s = i.strip()
      if s and not s.endswith(';'):
        try:
          output, ret = run_command(i, echo_output = True, name = name)
          print('Return Code:', ret)
        except KeyboardInterrupt:
          pass
        except (ValueError, TimeoutError) as exc:
          print('Failed to get result: ' + repr(exc))
    except BrokenPipeError:
      print('Lost connection to VM.')
      print('Exiting shell.')
      return

def _parse_size(sz):
  sz = str(sz).upper()
  m = re.match(r'\d+(B|K|M|G|T|P)?', sz)
  if not m:
    return None
  for idx, unit in enumerate('BKMGTP'):
    if sz[-1] == unit:
      return int(sz[:-1])*(1024**idx)
  return int(sz)

def _get_firmware_for_path(arch, uefi, sboot, nvram, path):
  try:
    manifests = map(lambda i: os.path.join(path, i),
                    sorted(os.listdir(path)))
  except FileNotFoundError:
    return None
  for manifest_path in manifests:
    try:
      with open(manifest_path, 'r') as f:
        manifest = json.load(f)
    except (json.JSONDecodeError,
            UnicodeDecodeError,
            IsADirectoryError,
            PermissionError):
      continue
    if uefi and 'uefi' not in manifest['interface-types']:
      continue
    if not uefi and 'bios' not in manifest['interface-types']:
      continue
    targets = manifest['targets']
    architectures = set((i['architecture'] for i in targets))
    if all((NORMALIZED_ARCHS.get(i, i) != arch  for i in architectures)):
      continue
    features = manifest.get('features')
    has_sboot = 'secure-boot' in features
    if sboot != has_sboot:
      continue
    mapping = manifest['mapping']
    if mapping['device'] != 'flash':
      continue
    binary = mapping['nvram-template' if nvram else 'executable']['filename']
    if os.path.isfile(binary):
      return binary
    bin = os.path.abspath(os.path.join(
            os.path.dirname(manifest_path),
            os.pardir,
            binary.rsplit('//')[-1]))
    if os.path.isfile(bin):
      return bin
  return None

def _get_firmware(arch, uefi, sboot, nvram):
  kind = 'uefi' if uefi else 'bios'
  fw = _C('{}_{}_firmware_path'.format(arch, kind))
  if fw:
    return fw
  backend = _get_backend(arch)
  if backend != 'qemu':
    raise NotImplementedError('Unsupported backend: {}'.format(backend))
  for path in PACKAGER_OVERWRITABLE_FIRMWARE_MANIFEST_PATHS:
    path = os.path.expandvars(path)
    bin = _get_firmware_for_path(arch, uefi, sboot, nvram, path)
    if bin:
      return bin
  out = subprocess.check_output((get_backend_cmd(arch), '-L', 'help'))
  for path in out.decode().splitlines():
    bin = _get_firmware_for_path(arch, uefi, sboot, nvram, path)
    if bin:
      return bin
    bin = _get_firmware_for_path(arch,
                                 uefi,
                                 sboot,
                                 nvram,
                                 os.path.join(path, 'firmware'))
    if bin:
      return bin
  e = 'Unable to find {} firmware for {}'.format(kind.upper(), arch)
  raise RuntimeError(e)

def get_tpm_backend(name = None):
  _apply_packager_overwrites()
  for backend in PACKAGER_OVERWRITABLE_TPM_BACKENDS:
    if _which(backend):
      return backend

def start_tpm(tpm_version, name = None):
  backend = get_tpm_backend(name = name)
  if not backend:
    raise RuntimeError('No TPM backends found - install one such as swtpm')
  if backend != 'swtpm':
    raise NotImplementedError()
  if tpm_version not in (1.2, 2.0, True):
    raise ValueError('Invalid TPM type: {}'.format(shlex.quote(tpm_type)))
  state_dir = _get_per_vm_dir_path(name, 'tpm_state')
  cmd = [_which(backend), 'socket', '--tpmstate', 'dir={}'.format(state_dir)]
  if is_unix:
    socket = os.path.join(state_dir, 'socket')
    cmd += ['--ctrl', 'type=unixio,path={}'.format(socket)]
    t = {'type': 'socket', 'path': socket}
  else:
    port = random.SystemRandom().randint(2000, 64000)
    cmd += ['--server', 'type=tcp,port={}'.format(port)]
    t = {'type': 'tcp', 'port': port, 'host': '127.0.0.1'}
  if tpm_version != 1.2:
    cmd += ['--tpm2']
  os.makedirs(state_dir, exist_ok = True)
  subprocess.Popen(cmd)
  return t

def _make_firmware_binary_path(name):
  return os.path.join(get_data_dir(), '{}_firmware.fd'.format(name))

def _make_whpx_bios_args(payload):
  name = payload.get('name')
  validate_name(name)
  fpath = _make_firmware_binary_path(name)
  try:
    size = os.stat(fpath).st_size
  except FileNotFoundError:
    size = 0
  buf = io.BytesIO()
  for i in ('nvram_template', 'firmware'):
    try:
      with open(payload.get(i), 'rb') as f:
        buf.write(f.read())
    except (FileNotFoundError, ValueError):
      pass
  if buf.tell() > size:
    with open(fpath, 'wb') as f:
      f.write(buf.getvalue())
  if size > 0 or buf.tell() > 0:
    return ['-bios', fpath]
  return []

def make_full_backend_cmd(payload):
  arch = payload['arch']
  backend = _get_backend(arch)
  if backend != 'qemu':
    raise NotImplementedError('Unsupported backend: {}'.format(backend))
  cmd = [get_backend_cmd(arch), '-serial', 'mon:stdio']
  if sys.platform == 'linux':
    cmd += ['-sandbox', 'on']
  narch = NORMALIZED_ARCHS.get(arch, arch)
  if narch in ('arm', 'aarch64', 'riscv'):
    cmd += ['-M', 'virt']
  if narch == 'x86_64':
    machine = platform.machine()
    machine = NORMALIZED_ARCHS.get(machine, machine)
    if narch == machine:
      cmd += ['-cpu', 'host']
    else:
      cmd += ['-cpu', 'EPYC']
  if os.name == 'nt':
    if _C('use_whpx'):
      cmd += (['-accel', 'whpx,kernel-irqchip=off'] + 
              _make_whpx_bios_args(payload))
  elif sys.platform == 'darwin':
    cmd += ['-accel', 'hvf']
  else:
    cmd += ['-accel', 'kvm', '-accel', 'xen', '-accel', 'nvmm']
  memory_unparsed = payload.get('memory')
  memory = _parse_size(memory_unparsed)
  if not memory and memory_unparsed:
    raise ValueError('invalid memory: {}'.format(memory_unparsed))
  if memory:
    cmd += ['-m', str(memory)+'B']
  for idx, disk in enumerate(payload.get('disks', [])):
    if type(disk) is str:
      disk = {'PATH': disk}
    disk_path = disk.get('PATH')
    if not disk_path:
      raise ValueError('Empty disk path given')
    disk_type = disk.get('TYPE', 'virtio').lower()
    if disk_type == 'virtio':
      cmd += ['-drive', 'if=virtio,file={}'.format(disk_path)]
    elif disk_type == 'nvme':
      i = 'nvme{}'.format(idx)
      cmd += ['-drive', 'if=none,id={},file={}'.format(i, disk_path)]
      cmd += ['-device', 'nvme,drive={},serial={}'.format(i, idx)]
    else:
      raise NotImplemented('Unsupported disk type: {}'.format(repr(disk_type)))
  for idx, disc in enumerate(payload.get('discs', [])):
    cmd += [
      '-drive', 'media=cdrom,index={},file={}'.format(idx, disc),
    ]
  fwd = []
  for port in payload.get('ports', []):
    protocol = port.get('protocol', 'tcp')
    host_port = port['host_port']
    guest_port = port['guest_port']
    fwd.append(',hostfwd={}::{}-:{}'.format(protocol, host_port, guest_port))
  fwd = ''.join(fwd)
  network_type = str(payload.get('network', None)).lower()
  if network_type == 'user':
    cmd += ['-device', 'virtio-net,netdev=net', '-netdev', 'user,id=net' + fwd]
  elif network_type == 'user_physical':
    cmd += ['-device', 'igb,netdev=net', '-netdev', 'user,id=net' + fwd]
  else:
    cmd += ['-nic', 'none']
  if payload.get('graphics'):
    cmd += ['-usbdevice', 'keyboard', '-device', 'usb-tablet']
  else:
    cmd.append('-nographic')
  if os.name != 'nt' or not _C('use_whpx'):
    firmware = payload.get('firmware')
    if firmware:
      cmd += ['-drive', 'if=pflash,format=raw,read-only=on,file='+firmware]
  if payload.get('prefer_discs'):
    cmd += ['-boot', 'd']
  tpm = payload.get('tpm')
  if tpm:
    cmd += ['-tpmdev', 'emulator,id=tpm0,chardev=chrtpm',
            '-device', 'tpm-tis,tpmdev=tpm0']
    tpm_type = tpm.get('type')
    if tpm_type == 'socket':
      cmd += ['-chardev', 'socket,id=chrtpm,path='+tpm['path']]
    elif tpm_type == 'tcp':
      c = 'socket,id=chrtpm,host='+tpm['host']+',port='+str(tpm['port'])
      if tpm.get('server'):
        c += ',server=on'
      cmd += ['-chardev', c]
    else:
      raise NotImplementedError('Unsupported TPM Type: {}'.format(repr(tpm_type)))
  # TODO vnc, share
  return cmd

def _cleanup_dead_vms_and_find_vm_with_lock(name = None):
  exists = False
  dead_vms = []
  for n, vm in _private.setdefault('vms', {}).items():
    r = vm['proc'].poll()
    if r is not None:
      dead_vms.append(n)
    elif n == name:
      exists = True
  for v in dead_vms:
    vm = _private['vms'].pop(v)
    try:
      with vm['lock']:
        for event in vm['events']:
          event.set()
    except KeyError:
      pass
  return exists

# TODO: test on Windows
# TODO: test to ensure exits correctly
def _stdio_reader(name):
  vm = _private['vms'][name]
  lock = vm['lock']
  proc = vm['proc']
  buf = vm['buf']
  events = vm['events']
  set_blocking = getattr(os, 'set_blocking', None)
  def_buf = _C('default_stdio_buffer_size') if set_blocking else 1
  max_buf = _C('max_stdio_buffer_size')
  blocking = True
  while True:
    b = proc.stdout.read(1 if blocking else def_buf)
    if not b:
      if blocking:
        return
    else:
      with lock:
        buf.append(b)
        buf_len = vm['buf_len'] + len(b)
        while buf_len > max_buf:
          b = buf.popleft()
          buf_len -= len(b)
          delta = max_buf - buf_len
          if delta > 0:
            b = b[delta:]
            if b:
              buf.appendleft(b)
              buf_len = max_buf
        vm['buf_len'] = buf_len
        for e in events:
          e.set()
    if b and blocking and set_blocking:
      blocking = False
      set_blocking(proc.stdout.fileno(), False)
    elif not b and not blocking and set_blocking:
      blocking = True
      set_blocking(proc.stdout.fileno(), True)

def _boot_internal(payload):
  name = payload['name']
  with _private['lock']:
    if _cleanup_dead_vms_and_find_vm_with_lock(name):
      raise AlreadyBootedError(name)
    if _private.get('no_new_vms'):
      raise RpcInterruptedError('Server is stopping')
    vm = {
      'proc': subprocess.Popen(
                make_full_backend_cmd(payload),
                bufsize = 0,
                stdin = subprocess.PIPE,
                stdout = subprocess.PIPE,
                stderr = subprocess.STDOUT,
              )
    }
    _private['vms'][name] = vm
  vm['lock'] = threading.Lock()
  vm['buf'] = collections.deque()
  vm['buf_len'] = 0
  vm['events'] = []
  threading.Thread(
    target = _stdio_reader,
    args = (name,),
    daemon = True
  ).start()

def _send_keys_internal(payload):
  name = payload['name']
  keys = b''.join(map(str.encode, payload['keys'])) # TODO: handle special keys
  with _private['lock']:
    try:
      stdin = _private['vms'][name]['proc'].stdin
    except KeyError:
      raise VmNotFoundError(name)
  stdin.write(keys)
  stdin.flush()

def _read_output_internal(payload):
  name = payload['name']
  event = threading.Event()
  try:
    vm = _private['vms'][name]
  except KeyError:
    raise VmNotFoundError(name)
  with vm['lock']:
    buf = vm['buf']
    r = b''.join(buf)
    if len(r) > 0:
      buf.clear()
      return base64.b85encode(r).decode()
    events = vm['events']
    events.append(event)
  event.wait(timeout = payload.get('timeout'))
  with vm['lock']:
    events.remove(event)
    r = b''.join(buf)
    if len(r) > 0:
      buf.clear()
      vm['buf_len'] = 0
  return base64.b85encode(r).decode()

def _stop_internal(name):
  with _private['lock']:
    try:
      _private['vms'][name]['proc'].terminate()
    except KeyError:
      raise VmNotFoundError(name)
    _cleanup_dead_vms_and_find_vm_with_lock()

def _boot(**kwargs):
  payload = {'action': 'boot'}
  payload.update(kwargs)
  _rpc_call(payload, can_start_daemon = True)

def manual_boot(**kwargs):
  kw = dict(kwargs)
  populate_default_boot_kwargs(kw)
  return _boot(**kw)

def boot(**kwargs):
  name = kwargs.get('name')
  vmdef_name = kwargs.get('vmdef_name', name)
  vm = _build(name, vmdef_name = vmdef_name)
  if vm.get('built') and vm.get('clean'):
    acquire_lock()
    state = load_state()
    state['vms'][name].pop('clean')
    write_state(state)
    release_lock()
  else:
    state = kwargs.get('state')
  vmdef = kwargs.get('vmdef')
  if not vmdef:
    vmdef = _get_vmdef_for_vm(vmdef_name, state)
  if not getattr(vmdef, 'SKIP_AUTO_BOOT', False):
    kw = dict(kwargs)
    kw.setdefault('uefi', True)
    populate_default_boot_kwargs(kw, vm = vm, vmdef = vmdef)
    _boot(**kw)
  if hasattr(vmdef, 'BOOT'):
    params = kwargs.get('custom_parameters')
    _start_catching_infinite_recursion(name, 'boot')
    try:
      vmdef.BOOT(VMProxy(name, custom_parameters = params))
    finally:
      _finish_catching_infinite_recursion(name, 'boot')

def ensure_booted(**kwargs):
  try:
    boot(**kwargs)
  except AlreadyBootedError:
    pass

_regex_special_chars = b'.*+(){}[]\\^$?|'

def _read_internal(pattern,
                   name = None,
                   encoding = None,
                   keep = False,
                   timeout = None,
                   callback = None):
  if type(pattern) is str:
    pattern = pattern.encode(encoding) if encoding else pattern.encode()
  with _private['lock']:
    buf = _private.setdefault('vms', {}).get(name, {}).get('buf', b'')
  max_buf = _C('max_stdio_buffer_size')
  match = None
  if callback:
    is_basic_string_pattern = all((c not in _regex_special_chars
                                   for c in pattern))
    if is_basic_string_pattern:
      cb_buf = b''
  while True:
    m = re.search(pattern, buf)
    if m:
      match = m
      break
    n = read_output(name = name, timeout = timeout)
    if not n:
      if callback and is_basic_string_pattern and cb_buf:
        callback(cb_buf)
      break
    buf += n
    if callback:
      if is_basic_string_pattern:
        n = cb_buf + n
        if n.startswith(pattern):
          n = b''
        else:
          for i in range(len(pattern), 0, -1):
            if n.endswith(pattern[:i]):
              cb_buf = n[-i:]
              n = n[:-i]
              break
      if n:
        callback(n)
    if not keep:
      buf = buf[-max_buf:]
  if keep:
    keep_buf = buf[:match.span()[1]] if match else b''
  if match:
    buf = buf[match.span()[1]:]
  with _private['lock']:
    if not match or not buf:
      try:
        _private['vms'][name].pop('buf')
      except KeyError:
        pass
    else:
      _private.setdefault('vms', {}).setdefault(name, {})['buf'] = buf
  if not keep:
    return match
  return keep_buf

def read_until(pattern,
               name = None,
               encoding = None,
               timeout = None,
               callback = None):
  return _read_internal(pattern,
                        name = name,
                        encoding = encoding,
                        keep = True,
                        timeout = timeout,
                        callback = callback)

def wait_for(pattern,
             name = None,
             encoding = None,
             timeout = None,
             callback = None):
  return _read_internal(pattern,
                        name = name,
                        encoding = encoding,
                        keep = False,
                        timeout = timeout,
                        callback = callback)

def wait_until_stopped(name = None, timeout = None):
  if timeout is not None:
    start = time.time()
  while True:
    _rpc_call({
      'action': 'wait_until_vm_stopped',
      'name': name,
      'timeout': timeout
    })
    still_running = name in list_running_vms()
    if not still_running:
      return not still_running
    if timeout is not None and (time.time() - start) >= timeout:
      return not still_running

def ensure_shell(verbosity = 1, name = None):
  if verbosity is not None:
    vprint(verbosity, 'Waiting for VM to boot', name = name)
  if name:
    vmdef = vmdef_from(name)
  systemimg = getattr(vmdef, 'SYSTEMIMG', None)
  if systemimg not in (None, DEFAULT_SYSTEMIMG):
    raise NotImplementedError('Unsupported systemimg: {}'.format(systemimg))
  wait_for('login:', name = name)
  if verbosity is not None:
    vprint(verbosity + 1, 'Logging in to shell', name = name)
  send_keys('root\n', name = name)
  wait_for('localhost:~# ', name = name)

_SYSTEM_SHELL_HELPERS_TEMPLATES = {
  'ensure_internet': ''' () (
    cat /etc/apk/repositories 2>/dev/null | grep :// 2>&1 >/dev/null \\
      || VV_type=apk
    [ "$VV_type" = "apk" ] && \\
      type setup-alpine >/dev/null && \\
      echo | setup-alpine -q
    [ "$VV_type" = "apk" ] && \\
        cat /etc/apk/repositories | \\
        sed '/community/ s/#http/http/g' > /etc/apk/repositories ;
    [ "x$VV_type" != "x" ] && unset VV_type && exit 0
    exit 1
  )
  ''',
  'ensure_has': ''' () {
    VNS_ensure_updated
    apk add --no-cache $*
  }
  ''',
  'ensure_updated': ''' () {
    VNS_ensure_internet
    apk update --no-cache
  }
  ''',
  'download_file': ''' () (
    type curl >/dev/null && curl $1 -o $2 && exit 0
    type wget >/dev/null && wget $1 -O $2 && exit 0
  )
  ''',
  'hash_file': ''' () (
    [ "$1" = "sha256" ] && type shasum >/dev/null
    [ "$?" = "0" ] && VV_hash="$(shasum -b -a 256 "$2")"
    [ "x$VV_hash" = "x" ] && [ "$1" = "sha256" ] && type sha256sum >/dev/null
    [ "$?" = "0" ] && VV_hash="$(sha256sum "$2")"
    VV_hash="$(echo $VV_hash | cut -d ' ' -f 1)"
    [ "x$VV_hash" != "x" ] && printf "$VV_hash" && exit 0
  )
  ''',
  'get_disk_at': ''' () {
    DISK_CODE
  }
  ''',
  'mount_disk_at': ''' () (
    VV_ret=1
    VV_disk="$(VNS_get_disk_at $1 $2)"
    for VV_fstype in auto vfat ext4 btrfs ntfs-3g ufs udf ntfs
    do
      [ "$VV_ret" != "0" ] \\
        && [ "x$VV_disk" != "x" ] \\
        && mount -t $VV_fstype "$VV_disk" $3 2>/dev/null && VV_ret=0
    done
    exit $VV_ret
  )
  ''',
  'unmount_disk_at': ''' () {
    VV_disk="$(VNS_get_disk_at $1 $2)"
    [ "x$VV_disk" != "x" ] && umount $VV_disk
    unset VV_disk
  }
  ''',
  'get_disc_at': ''' () {
    printf "/dev/sr$1"
  }
  ''',
  'mount_disc_at': ''' () (
    VV_ret=1
    VV_disc="$(VNS_get_disc_at $1 $2)"
    for VV_fstype in udf auto vfat ext4 btrfs ntfs-3g ufs ntfs
    do
      [ "$VV_ret" != "0" ] \\
        && [ "x$VV_disc" != "x" ] \\
        && mount -t $VV_fstype "$VV_disc" $3 2>/dev/null && VV_ret=0
    done
    exit $VV_ret
  )
  ''',
  'unmount_disc_at': ''' () {
    VV_disc="$(VNS_get_disc_at $1 $2)"
    [ "x$VV_disc" != "x" ] && umount $VV_disc
    unset VV_disc
  }
  ''',
  'get_arch': ''' () {
    VV_arch="$(uname -m)"
    ARCH_CODE
    printf "$VV_arch"
    unset VV_arch
  }
  '''
}

def _make_disk_letters(idx):
  res = ''
  while True:
    res = string.ascii_lowercase[idx % 26] + res
    idx //= 26
    if idx < 1:
      return res
    idx -= 1

def make_system_shell_helpers(include = None,
                              namespace = None,
                              name = None):
  ns = (str(namespace) + '_') if namespace else ''
  if include:
    include = [i[len(ns):] if i.startswith(ns) else i for i in include]
  all_funcs = set(_SYSTEM_SHELL_HELPERS_TEMPLATES.keys())
  needed = set(include) if include else all_funcs
  while True:
    ineeded = set(needed)
    for fname in ineeded:
      code = _SYSTEM_SHELL_HELPERS_TEMPLATES[fname]
      needed.update(filter(lambda i: ('VNS_' + i) in code, all_funcs))
    if len(ineeded) == len(needed):
      break
  import textwrap
  script = ''.join((
    '{} {}\n\n'.format(
      ns + i,
      textwrap.dedent(
        '  ' + _SYSTEM_SHELL_HELPERS_TEMPLATES[i]
          .strip()
          .replace('VNS_', ns)
      )
    ) for i in needed
  ))
  if 'get_disk_at' in needed:
    disk_code = []
    acquire_lock()
    state = load_state()
    release_lock()
    vmdef = _get_vmdef_for_vm(name, state)
    disk_defs = getattr(vmdef, 'DISKS', [])
    virtio_idx = 0
    nvme_idx = 0
    disks = state.get('vms', {}).get(name, {}).get('disks', [])
    for idx, disk in enumerate(disks):
      try:
        disk_def = disk_defs[idx]
      except IndexError:
        disk_def = {}
      disk_type = disk_def.get('TYPE', 'virtio').lower()
      if disk_type == 'virtio':
        d = _make_disk_letters(virtio_idx)
        disk_code.extend((
          '[ "x$1" = "x{}" ] && VV_disk=/dev/vd{}'.format(idx, d),
          '[ "x$1" = "x{}" ] && VV_disk_type=virtio'.format(idx),
        ))
        virtio_idx += 1
      elif disk_type == 'nvme':
        disk_code.extend((
          '[ "x$1" = "x{}" ] && VV_disk=/dev/nvme{}n1'.format(idx, nvme_idx),
          '[ "x$1" = "x{}" ] && VV_disk_type=nvme'.format(idx),
        ))
        nvme_idx += 1
    disk_code.append(
      '[ "$VV_disk_type" = "virtio" ] && [ "x$2" != "x" ] && ' +
      'VV_disk="$VV_disk""$(expr $2 + 1)"'
    )
    disk_code.append(
      '[ "$VV_disk_type" = "nvme" ] && [ "x$2" != "x" ] && ' +
      'VV_disk="$VV_disk""p""$(expr $2 + 1)"'
    )
    disk_code.append('[ "x$VV_disk" != "x" ] && printf "$VV_disk"')
    disk_code.append('unset VV_disk')
    disk_code.append('unset VV_disk_type')
    script = script.replace('DISK_CODE', '\n  '.join(disk_code))
  if 'get_arch' in needed:
    arch_code = []
    for k, v in NORMALIZED_ARCHS.items():
      arch_code.append(
        '[ "$VV_arch" = {} ] && VV_arch={}'.format(
          shlex.quote(k),
          shlex.quote(v))
      )
    script = script.replace('ARCH_CODE', '\n'.join(arch_code))
  return script.replace('\\\n', '').replace('VV', generate_id())

def prepend_system_shell_helpers(script,
                                 include = None,
                                 namespace = None,
                                 name = None):
  needed = set(include if include else [])
  ns = (str(namespace) + '_') if namespace else ''
  for fname in _SYSTEM_SHELL_HELPERS_TEMPLATES.keys():
    if (ns + fname) in script:
      needed.add(fname)
  if needed:
    return make_system_shell_helpers(include = needed,
                                     namespace = namespace,
                                     name = name) + script
  return script

def _echo_shell_callback(buf, indent = '  ', tab = '    ', state = None):
  try:
    buf = buf.decode()
  except UnicodeDecodeError:
    return
  width = state.get('width')
  if not width:
    width, _ = shutil.get_terminal_size()
    state['width'] = width
  current_line = io.StringIO()
  finished_lines = []
  pos = state.get('pos', 0)
  for c in buf:
    if c == '\n':
      finished_lines.append(current_line.getvalue())
      current_line.truncate(0)
      pos = 0
    elif c == '\r':
      current_line.write('\r' + indent)
      pos = len(indent)
    elif c == '\b':
      if pos > len(indent):
        current_line.write('\b')
        pos -= 1
    elif c == '\t':
      delta = width - pos
      if delta < len(tab):
        remaining = indent + tab[delta:]
        current_line.write(tab[:delta])
        finished_lines.append(current_line.getvalue())
        current_line.seek(0)
        current_line.write(remaining)
        current_line.truncate()
        pos = len(remaining)
      else:
        current_line.write(tab)
        pos += len(tab)
    elif c == '\a':
      current_line.write('\a')
    elif c.isprintable():
      if pos < 1:
        current_line.write(indent)
        pos = len(indent)
      current_line.write(c)
      pos += 1
    if pos >= width:
      finished_lines.append(current_line.getvalue())
      current_line.truncate(0)
      pos = 0
  sys.stdout.write('\n'.join((
    [line if line else indent for line in finished_lines] +
    [current_line.getvalue()]
  )))
  sys.stdout.flush()
  state['pos'] = pos

def _run_command(command,
                 _input = None,
                 name = None,
                 timeout = None,
                 callback = None,
                 echo_output = False,
                 echo_indent = '  ',
                 echo_tab = '    ',
                 echo_verbosity = 2,
                 check = False):
  pre_a = generate_id()
  pre_b = generate_id()
  post_a = generate_id()
  post_b = generate_id()
  ret_var = generate_id()
  ret_delim_a = generate_id()
  ret_delim_b = generate_id()
  cmd  = 'printf {};printf {};'.format(pre_a, pre_b)
  cmd += '{};{}=$?;'.format(command, ret_var)
  cmd += 'printf {};printf {};'.format(post_a, post_b)
  cmd += 'printf "${} {}";'.format(ret_var, ret_delim_a)
  cmd += 'printf {};unset {}\n'.format(ret_delim_b, ret_var)
  send_keys(cmd, name = name)
  buf = b''
  pre = (pre_a + pre_b).encode()
  if _input:
    if hasattr(_input, 'decode'):
      _input = _input.decode()
    buf += read_until(pre, name = name, timeout = timeout)
    for c in _input:
      send_keys(c, name = name)
  post = (post_a + post_b).encode()
  if callback is None and echo_output and _should_vprint(echo_verbosity):
    cb_state = {}
    idnt = _vprint_prefix(name = name) + echo_indent
    callback = lambda b, i = idnt, t = echo_tab, s = cb_state: \
                 _echo_shell_callback(b, indent = i, tab = t, state = s)
  else:
    cb_state = None
  if callback:
    if not _input:
      buf += read_until(pre, name = name, timeout = timeout)
    buf += read_until(post,
                      name = name,
                      timeout = timeout,
                      callback = callback)
  ret_delim = (ret_delim_a + ret_delim_b).encode()
  buf += read_until(ret_delim, name = name, timeout = timeout)
  try:
    start = buf.index(pre) + len(pre)
    end = buf.index(post)
    end_ret = buf.index(ret_delim)
  except ValueError as exc:
    if timeout is not None:
      e = 'command {} on {} timed out'.format(shlex.quote(command),
                                              shlex.quote(name))
      raise TimeoutError(errno.ETIMEDOUT, e)
    else:
      raise exc  
  output = buf[start:end]
  ret = int(buf[end + len(post):end_ret].strip())
  if cb_state and cb_state.get('pos'):
    sys.stdout.write('\n')
    sys.stdout.flush()
  if check and ret != 0:
    raise CalledProcessError(ret, command, _input, output)
  return output, ret

def clear_history(timeout = None,
                  callback = None,
                  echo_output = False,
                  echo_indent = '  ',
                  echo_tab = '    ',
                  echo_verbosity = 2,
                  name = None):
  return _run_command('unset HISTFILE',
                      timeout = timeout,
                      callback = callback,
                      echo_output = echo_output,
                      echo_indent = echo_indent,
                      echo_tab = echo_tab,
                      echo_verbosity = echo_verbosity,
                      check = True,
                      name = name)

def run_command(command,
                timeout = None,
                callback = None,
                echo_output = False,
                echo_indent = '  ',
                echo_tab = '    ',
                echo_verbosity = 2,
                check = False,
                clear_history = True,
                name = None):
  if '\n' in command:
    raise ValueError('line break in command: {}'.format(repr(command)))
  if clear_history:
    globals().get('clear_history')(timeout = timeout,
                                   callback = callback,
                                   echo_output = echo_output,
                                   echo_tab = echo_tab,
                                   echo_verbosity = echo_verbosity,
                                   name = name)
  return _run_command(command,
                      timeout = timeout,
                      callback = callback,
                      echo_output = echo_output,
                      echo_indent = echo_indent,
                      echo_tab = echo_tab,
                      echo_verbosity = echo_verbosity,
                      check = check,
                      name = name)

def pipe_string(pipe_in,
                command,
                chunk_size = 100,
                timeout = None,
                callback = None,
                echo_output = False,
                echo_indent = '  ',
                echo_tab = '    ',
                echo_verbosity = 2,
                check = False,
                clear_history = True,
                name = None):
  if hasattr(pipe_in, 'encode'):
    pipe_in = pipe_in.encode()
  if len(pipe_in) > 1048576: # 1 MB
    raise ValueError('input string too long (consider put_file_data instead)')
  if '\n' in command:
    raise ValueError('line break in command: {}'.format(repr(command)))
  buf = pipe_in
  buf_var = generate_id()
  if clear_history:
    globals().get('clear_history')(timeout = timeout,
                                   callback = callback,
                                   echo_output = echo_output,
                                   echo_tab = echo_tab,
                                   echo_verbosity = echo_verbosity,
                                   name = name)
  while len(buf) > 0:
    chunk = buf[:chunk_size]
    buf = buf[chunk_size:]
    enc = ''.join(('\\'+oct(b)[2:] for b in chunk))
    cmd = '{}="${}""$(printf {})"'.format(buf_var, buf_var, shlex.quote(enc))
    _, ret = _run_command(cmd, timeout = timeout, name = name)
    if ret != 0:
      raise RuntimeError('buffer write failed')
  delim = generate_id()
  count = len(pipe_in)
  cmd = '(cat <<{}\n${}\n{}\n) | dd ibs=1 count={} 2>/dev/null | {}'
  cmd = cmd.format(delim, buf_var, delim, count, command)
  output, ret = _run_command(cmd,
                             timeout = timeout,
                             callback = callback,
                             echo_output = echo_output,
                             echo_indent = echo_indent,
                             echo_tab = echo_tab,
                             echo_verbosity = echo_verbosity,
                             name = name)
  _run_command('unset {}'.format(buf_var), timeout = timeout, name = name)
  if check and ret != 0:
    raise CalledProcessError(ret, command, pipe_in, output)
  return output, ret

def pipe_shell(script,
               shell = '${SHELL:-sh}',
               timeout = None,
               callback = None,
               echo_output = False,
               echo_indent = '  ',
               echo_tab = '    ',
               echo_verbosity = 2,
               check = False,
               clear_history = True,
               name = None):
  return pipe_string(script,
                     shell,
                     timeout = timeout,
                     callback = callback,
                     echo_output = echo_output,
                     echo_indent = echo_indent,
                     echo_tab = echo_tab,
                     echo_verbosity = echo_verbosity,
                     check = check,
                     clear_history = clear_history,
                     name = name)

def _make_remote_file(path,
                      replace,
                      timeout = None,
                      clear_history = True,
                      name = None, ):
  r = '+' if replace else '-'
  cmd = '(set {}o noclobber;printf "" > "{}")'.format(r, path)
  if clear_history:
    globals().get('clear_history')(timeout = timeout, name = name)
  output, ret = _run_command(cmd, timeout = timeout, name = name)
  if not replace and ret != 0:
    raise FileExistsError(path)
  if ret != 0:
    raise RuntimeError('unexpected return code: {}'.format(ret))

def _append_remote_file(name, path, chunk, timeout = None):
  enc = ''.join(('\\'+oct(b)[2:] for b in chunk))
  cmd = '(printf \'{}\') >> "{}"'.format(enc, path)
  output, ret = _run_command(cmd, timeout = timeout, name = name)
  if ret != 0:
    raise RuntimeError('unexpected return code: {}'.format(ret))

def put_file_data(path,
                  data,
                  timeout = None,
                  replace = False,
                  chunk_size = 100,
                  clear_history = True,
                  name = None):
  buf = data.encode() if hasattr(data, 'encode') else data
  _make_remote_file(path,
                    replace = replace,
                    timeout = timeout,
                    clear_history = clear_history,
                    name = name)
  while len(buf) > 0:
    chunk = buf[:chunk_size]
    buf = buf[chunk_size:]
    _append_remote_file(name, path, chunk, timeout = timeout)

def put_file(src_path,
             dst_path,
             replace = False,
             chunk_size = 100,
             clear_history = True,
             name = None):
  with open(src_path, 'rb') as fh:
    _make_remote_file(dst_path,
                      replace = replace,
                      timeout = timeout,
                      clear_history = clear_history,
                      name = name)
    while True:
      chunk = fh.read(chunk_size)
      if not chunk:
        break
      _append_remote_file(name, dst_path, chunk, timeout = timeout)

def get_file_data(src_path,
                  count = None,
                  offset = None,
                  timeout = None,
                  clear_history = True,
                  name = None):
  count_arg = '' if count is None else (' -N ' + str(int(count)))
  offset_arg = '' if offset is None else (' -j' + str(int(offset)))
  cmd = 'od -A n{}{} -t x1 -v {}'.format(offset_arg, count_arg, repr(src_path))
  if clear_history:
    globals().get('clear_history')(timeout = timeout, name = name)
  output, ret = _run_command(cmd, timeout = timeout, name = name)
  if ret != 0:
    raise FileNotFoundError(src_path)
  buf = io.BytesIO()
  for line in output.splitlines():
    _bytes = line.split()
    for b in _bytes:
      buf.write(bytes.fromhex(b.decode()))
  buf.seek(0)
  return buf.read()

def get_file(src_path,
             dst_path,
             replace = False,
             chunk_size = 1024,
             timeout = None,
             clear_history = True,
             name = None):
  offset = 0
  chunk_size = int(chunk_size)
  if chunk_size < 1:
    raise ValueError('Invalid chunk_size: {}'.format(chunk_size))
  mode = 'wb' if replace else 'xb'
  with open(dst_path, mode) as fh:
    while True:
      ch = clear_history and (offset < 1)
      chunk = get_file_data(src_path,
                            offset = offset,
                            count = chunk_size,
                            timeout = timeout,
                            clear_history = ch,
                            name = name)
      fh.write(chunk)
      if len(chunk) < chunk_size:
        return
      offset += chunk_size

def acpi_shutdown(name = None, timeout = None):
  backend = _get_backend(vmdef = vmdef_from(name))
  if backend == 'qemu':
    send_keys(CTRL_A + 'c\n', name = name)
    if timeout is not None:
      deadline = time.time() + timeout
    match = wait_for(r'\(qemu\) ', name = name, timeout = timeout)
    if not match:
      return force_stop(name = name)
    send_keys('system_powerdown\n', name = name)
    if timeout is not None:
      t = deadline - time.time()
    else:
      t = None
    match = wait_for(r'\(qemu\) ', name = name, timeout = t)
    if not match:
      return force_stop(name = name)
    send_keys(CTRL_A + 'c\n', name = name)
  else:
    raise NotImplementedError('Unsupported backend: {}'.format(backend))

def list_running_vms():
  try:
    return _rpc_call({'action': 'list_running'})
  except RpcTimoutError:
    return []

def is_running(name = None):
  return name in list_running_vms()

def list_all_vms(state = None):
  if state is None:
    acquire_lock()
    state = load_state()
    release_lock()
  return list(state.get('vms', {}).keys())

def list_registered_vmdefs(state = None):
  if state is None:
    acquire_lock()
    state = load_state()
    release_lock()
  return list(state.get('vmdefs', {}).keys())

def list_well_known_vmdefs():
  vmdefs = set()
  lib_path = os.path.dirname(os.path.realpath(__file__))
  for r in (lib_path, get_data_dir()):
    try:
      for i in os.listdir(os.path.join(r, 'well_known_vmdefs')):
        if i.endswith('.vmdef'):
          vmdefs.add(i[:-6])
    except FileNotFoundError:
      pass
  import tarfile
  try:
    with tarfile.open(os.path.join(get_data_dir(), 'well_known_vmdefs.tar.gz')) as tf:
      for member in tf.getmembers():
        if member.name.endswith('.vmdef'):
          vmdefs.add(member.name[:-6])
  except FileNotFoundError:
    pass
  return list(vmdefs)

def list_all_vmdefs(state = None):
  return list(set(list_registered_vmdefs(state = state) +
                  list_installed_vmdefs() +
                  list_well_known_vmdefs()))

def send_keys(keys, name = None):
  keys = keys.decode() if hasattr(keys, 'decode') else keys
  _rpc_call({'action': 'send_keys', 'name': name, 'keys': list(keys)})

def read_output(name = None, timeout = None):
  payload = {'action': 'read_output', 'name': name, 'timeout': timeout}
  return base64.b85decode(_rpc_call(payload))

def print_help(args = []):
  rc = subprocess.check_call((sys.executable, '-m', 'pydoc', *args, __file__))
  return rc

def _do_help_command(vm_name, vmdef_name, command_args, custom_parameters):
  return print_help(args = ['-w'] if _private.get('write_html') else [])

def get_forwarded_ports():
  return _private.get('ports', [])

def merge_ports(child, parent = None, allow_overwrite = False, name = None):
  ports_by_host_port = {}
  ports_by_guest_port = {}
  if parent is None:
    parent = get_forwarded_ports()
  for port in (parent + child):
    proto = shlex.quote(port.get('protocol', 'TCP').upper())
    for kind, mapping in (('host', ports_by_host_port),
                          ('guest', ports_by_guest_port)):
      num = repr(port.get(kind + '_port'))
      key = '{}:{}'.format(proto, num)
      if not allow_overwrite:
        old = mapping.get(key)
        if old and old != port:
          e = '{} {} port assigned twice: {}'.format(kind, proto, num)
          raise ValueError(e)
      mapping[key] = port
  merged = {}
  for port in ports_by_host_port.values():
    merged[(port.get('protocol', 'TCP').upper(),
            port.get('guest_port'))] = port
  return list(merged.values())

def run(*args, inherit_args = True, check = True, **kwargs):
  cmd = [sys.executable, __file__]
  if len(args) > 0:
    a = args[0]
    cmd += list(a) if type(a) in (list, tuple) else shlex.split(a)
  if inherit_args:
    iargs = []
    if _private.get('force_graphics'):
      iargs += ['--graphics']
    for port in _private.get('ports', []):
      protocol = port['protocol']
      host_port = str(port['host_port'])
      guest_port = str(port['guest_port'])
      iargs += ['--port', protocol, host_port, guest_port]
    cmd = cmd[:2] + iargs + cmd[2:]
  args = [cmd] + list(args[1:])
  return subprocess.run(*args, check = check, **kwargs)

_CURRENT_VIRTUATOR_EXPORT_VERSION = 1
_DEFAULT_EXPORT_EXTENSION = '.vm.tar.gz'
_DEFAULT_EXPORT_NAME = 'multiple'
_EXPORTABLE_BOOLS = ('built', 'clean')

def export(name = None, names = None, output_path = None):
  names = ([name] if name else []) + (names or [])
  if not names:
    raise ValueError('No names given - nothing to export')
  acquire_lock()
  state = load_state()
  release_lock()
  file_queue = []
  dir_queue = []
  vmdef_queue = []
  metadata = {
    'virtuator_export_version': _CURRENT_VIRTUATOR_EXPORT_VERSION,
    'vms': {},
  }
  state_vms = state.get('vms', {})
  well_knowns = list_well_known_vmdefs()
  for name in names:
    if name in metadata['vms']:
      continue
    vm_state = state_vms.get(name)
    if vm_state is None:
      raise VmNotFoundError(name)
    vm = {}
    disks = []
    for idx in range(len(vm_state.get('disks', []))):
      for clean in (False, True):
        disk = make_disk_path(name, idx, clean = clean)
        bn = os.path.basename(disk)
        if not clean or os.path.isfile(disk):
          file_queue.append((disk, bn))
        if not clean:
          disks.append(bn)
    if disks:
      vm['disks'] = disks
    for k in _EXPORTABLE_BOOLS:
      v = vm_state.get(k)
      if v:
        vm[k] = v
    vmdef_name = vm_state.get('vmdef_name')
    if vmdef_name:
      vm['vmdef_name'] = vmdef_name
    vmdef_name = vmdef_name or name
    if vmdef_name not in well_knowns:
      vmdef_queue.append(vmdef_name)
    metadata['vms'][name] = vm
    for sfx in _VM_DIR_SUFFIXES:
      path = _get_per_vm_dir_path(name, sfx)
      dir_queue.append((path, os.path.basename(path)))
  if vmdef_queue:
    dir_name = os.path.basename(_get_installed_vmdef_dir())
    seen = set()
    while vmdef_queue:
      vmdef_name = vmdef_queue.pop()
      if vmdef_name in seen:
        continue
      vmdef = vmdef_from(vmdef_name)
      if not vmdef:
        raise VmdefNotFoundError(vmdef_name)
      path = vmdef.__file__
      file_queue.append(path, '{}/{}.vmdef'.format(dir_name, vmdef_name))
      requires = getattr(vmdef, 'REQUIRES', [])
      requires = requires if type(requires) is list else [requires]
      for r in requires:
        if r not in well_knowns:
          vmdef_queue.append(r)
      seen.add(vmdef_name)
  import io, tarfile
  info = tarfile.TarInfo(name = 'metadata.json')
  metadata = json.dumps(metadata).encode()
  info.size = len(metadata)
  tf = None
  default_name = ((names[0] if len(names) == 1 else _DEFAULT_EXPORT_NAME) +
                   _DEFAULT_EXPORT_EXTENSION)
  if not output_path:
    output_path = default_name
  try:
    tf = tarfile.open(name = output_path, mode = 'x:gz')
  except IsADirectoryError:
    tf = tarfile.open(name = os.path.join(output_path, default_name),
                      mode = 'x:gz')
  try:
    tf.addfile(info, io.BytesIO(metadata))
    for queue, is_dir in ((dir_queue, True), (file_queue, False)):
      seen = set()
      while queue:
        in_path, output_path = queue.pop()
        in_path = os.path.abspath(in_path)
        if in_path in seen:
          continue
        info = tarfile.TarInfo(name = output_path)
        if is_dir:
          info.type = tarfile.DIRTYPE
          try:
            with os.scandir(in_path) as it:
              for entry in it:
                p = entry.path
                op = '{}/{}'.format(output_path, os.path.basename(p))
                (dir_queue if entry.is_dir() else file_queue).append((p, op))
            tf.addfile(info)
          except FileNotFoundError:
            pass
        else:
          with open(in_path, 'rb') as f:
            info.size = os.fstat(f.fileno()).st_size
            tf.addfile(info, f)
        seen.add(in_path)
  finally:
    if tf:
      tf.close()

def _printable_safe_char(c):
  if c == '\t':
    return '  '
  if c == '\n':
    return '\n'
  return c if c.isprintable() else ''

def _printable_safe_read(fh, encoding = None):
  buf = []
  wrapper = io.TextIOWrapper(fh, encoding = encoding)
  while True:
    try:
      b = wrapper.read(_C('disk_buffer_size'))
    except UnicodeDecodeError:
      fh.seek(0, io.SEEK_END)
      size = fh.tell()
      return ('<binary file ({}) - no preview available>'
                .format(human_size(size)))
    if not b:
      break
    buf.append(''.join((_printable_safe_char(c) for c in b)))
  return ''.join(buf)

def _import_member(tf, member, path, replace, dry_run_summaries):
  ifh = tf.extractfile(member)
  if dry_run_summaries is not None:
    if replace and os.path.exists(path):
      dry_run_summaries.append('REPLACE existing path {}'.format(repr(path)))
    dry_run_summaries.append('Extract {} from {} to {}'
                              .format(repr(member.name),
                                      repr(tf.name),
                                      repr(path)))
    dry_run_summaries.append('# Start of {} from {}'
                              .format(repr(member.name),
                                      repr(tf.name)))
    dry_run_summaries.append(_printable_safe_read(ifh))
    dry_run_summaries.append('# End of {} from {}'
                              .format(repr(member.name),
                                      repr(tf.name)))
  else:
    with open(path, 'wb' if replace else 'xb') as of:
      while True:
        buf = ifh.read(_C('disk_buffer_size'))
        if not buf:
          break
        of.write(buf)

def import_vms(path = None,
               paths = None,
               replace_vms = False,
               replace_vmdefs = False,
               dry_run = False,
               custom_parameters = None):
  paths = ([path] if path else []) + (paths if paths else [])
  import tarfile
  well_knowns = list_well_known_vmdefs()
  vmdef_dir = _get_installed_vmdef_dir()
  vmdef_dir_name = os.path.basename(vmdef_dir)
  if dry_run:
    summaries = []
  for path in paths:
    with tarfile.open(path) as tf:
      members = {m.name: m for m in tf.getmembers()}
      metadata = json.load(tf.extractfile(members['metadata.json']))
      version = metadata['virtuator_export_version']
      if version > _CURRENT_VIRTUATOR_EXPORT_VERSION:
        e = '{} is from a newer version of Virtuator ({}) - {}'.format(
          repr(tf.name), repr(version), 'please update Virtuator',
        )
        raise RuntimeError(e)
      file_map = {}
      dir_map = {}
      install_required = False
      for vm_name, vm in metadata['vms'].items():
        validate_name(vm_name)
        validated_vm = {}
        if dry_run:
          summaries.append('Import VM {} from {}'
                           .format(repr(vm_name), repr(tf.name)))
        disks = vm.get('disks')
        if disks:
          vdisks = []
          for idx, in_path in enumerate(disks):
            out_path = make_disk_path(vm_name, idx, clean = False)
            file_map[in_path] = (out_path, replace_vms)
            vdisks.append(out_path)
            if dry_run:
              summaries.append('Copy disk {} ({}) for {} from {}'
                               .format(repr(in_path),
                                       human_size(members[in_path].size),
                                       repr(vm_name),
                                       repr(tf.name)))
            out_path = make_disk_path(vm_name, idx, clean = True)
            in_path = os.path.basename(out_path)
            member = members.get(in_path)
            if member:
              file_map[in_path] = (out_path, replace_vms)
              if dry_run:
                vm_summary.append('Copy clean disk {} ({}) for {} from {}'
                                  .format(repr(in_path),
                                          human_size(member.size),
                                          repr(vm_name),
                                          repr(tf.name)))
          validated_vm['disks'] = vdisks
        for k in _EXPORTABLE_BOOLS:
          if vm.get(k):
            validated_vm[k] = True
        vmdef_name = vm.get('vmdef_name')
        if vmdef_name and vmdef_name != vm_name:
          validate_name(vmdef_name)
          if vmdef_name in well_knowns:
            validated_vm['vmdef_name'] = vmdef_name
          else:
            fname = '{}.vmdef'.format(vmdef_name)
            in_path = '{}/{}'.format(vmdef_dir_name, fname)
            member = members.get(in_path)
            if member:
              validated_vm['vmdef_name'] = in_path
              vmdef = vmdef_from(vmdef_name)
              if not vmdef:
                file_map[in_path] = (os.path.join(vmdef_dir, fname), replace_vmdefs)
                if dry_run:
                  summaries.append('Install {} from {} for VM {}'
                                   .format(repr(in_path),
                                           repr(tf.name),
                                           repr(vm_name)))
                  summaries.append('# Start of {}'.format(repr(in_path)))
                  summaries.append(_printable_safe_read(tf.extractfile(member)))
                  summaries.append('# End of {}'.format(repr(in_path)))
                else:
                  install_required = True
        for sfx in _VM_DIR_SUFFIXES:
          out_dir = _get_per_vm_dir_path(vm_name, sfx)
          dir_map[os.path.basename(out_dir)] = out_dir
        acquire_lock()
        try:
          state = load_state()
          vms = state.setdefault('vms', {})
          if vm_name in vms:
            if not replace_vms:
              raise VmExistsError(vm_name)
            if dry_run:
              summaries.append('REMOVE existing VM {}'.format(repr(vm_name)))
            else:
              rm(vm_name, custom_parameters = custom_parameters)
          vms[vm_name] = validated_vm
          if not dry_run:
            write_state(state)
        finally:
          release_lock()
      if install_required:
        os.makedirs(vmdef_dir, exist_ok = True)
      imported = set()
      dry_run_summaries = summaries if dry_run else None
      for in_path, member in members.items():
        r = file_map.get(in_path)
        if r:
          out_path, replace = r
          _import_member(tf, member, out_path, replace, dry_run_summaries)
          continue
        sp = in_path.split('/')
        out_path = dir_map.get(sp[0])
        if out_path:
          if out_path not in imported and not dry_run:
            os.mkdir(out_path)
            imported.add(out_path)
          is_dir = member.isdir()
          for i in sp[1:None if is_dir else -1]:
            out_path = os.path.join(out_path, i)
            if out_path not in imported and not dry_run:
              os.mkdir(out_path)
              imported.add(out_path)
          if not is_dir:
            _import_member(tf,
                           member,
                           os.path.join(out_path, sp[-1]),
                           replace_vms,
                           dry_run_summaries)
  if dry_run:
    return '\n\n'.join(summaries)

def _validate_vmdefs_paths(paths):
  code_cache = {}
  for path in paths:
    with open(path, 'rb') as f:
      code_cache[path] = f.read()
  name_cache = {}
  for path, code in code_cache.items():
    vmdef = try_load_vmdef_code('x', code, location = path, cache = False)
    if not vmdef:
      raise ValueError('{} is not a valid vmdef'.format(repr(path)))
    name_cache[path] = vmdef.NAME
  return code_cache, name_cache

def install_vmdefs(path = None, paths = None, replace_vmdefs = False):
  paths = ([path] if path else []) + (paths if paths else [])
  dir_created = False
  install_dir = _get_installed_vmdef_dir()
  code_cache, name_cache = _validate_vmdefs_paths(paths)
  recoverable_cache = {}
  acquire_lock()
  try:
    for path, code in code_cache.items():
      name = name_cache.get(path)
      if name is None:
        continue
      install_path = os.path.join(install_dir, name + '.vmdef')
      if replace_vmdefs:
        try:
          with open(install_path, 'rb') as f:
            old_vmdef_code = f.read()
        except FileNotFoundError:
          old_vmdef_code = None
        while old_vmdef_code is not None:
          try:
            with open(
              '{}.{}.bak'.format(install_path, os.urandom(10).hex()),
              'xb') as f:
              f.write(old_vmdef_code)
              recoverable_cache[install_path] = f.name
              break
          except FileExistsError:
            pass
      if not dir_created:
        os.makedirs(install_dir, exist_ok = True)
        dir_created = True
      with open(install_path, 'wb' if replace_vmdefs else 'xb') as f:
        f.write(code)
  finally:
    for old_path, new_path in recoverable_cache.items():
      try:
        sz = os.path.getsize(old_path)
      except FileNotFoundError:
        sz = 0
      if sz < 1:
        try:
          os.remove(old_path)
        except FileNotFoundError:
          pass
        os.rename(new_path, old_path)
    release_lock()

def list_installed_vmdefs():
  names = []
  try:
    for fname in os.listdir(_get_installed_vmdef_dir()):
      if not fname.endswith('.vmdef'):
        p = repr(os.path.join(_get_installed_vmdef_dir(), fname))
        raise RuntimeError('Invalid file in installed VMDEF dir: {}'.format(p))
      names.append(fname[:-6])
  except FileNotFoundError:
    pass
  return names

def _get_vmdef_dependency_tree_with_lock(state = None):
  if state is None:
    state = load_state()
  well_knowns = list_well_known_vmdefs()
  user_vmdefs = list_registered_vmdefs(state = state) + list_installed_vmdefs()
  tree = {i: {'vmdefs': [], 'vms': []} for i in user_vmdefs}
  for name in user_vmdefs:
    vmdef = vmdef_from(name, state = state)
    if not vmdef:
      raise RuntimeError('Unable to load VMDEF {}'.format(repr(name)))
    requires = getattr(vmdef, 'REQUIRES', [])
    if type(requires) is str:
      requires = [requires]
    for requirement in requires:
      if requirement not in well_knowns:
        tree[requirement]['vmdefs'].append(name)
  for vm_name, vm in state.get('vms', {}).items():
    vmdef_name = vm.get('vmdef_name', vm_name)
    if vmdef_name in well_knowns:
      continue
    node = tree.get(vmdef_name)
    if node is None:
      raise RuntimeError('VM {} depends on unknown VMDEF {}'
                           .format(repr(vm_name), repr(vmdef_name)))
    node['vms'].append(vm_name)
  return tree

def _verify_vmdefs_are_not_dependencies_with_lock(names, state = None):
  name_set = set(names)
  tree = _get_vmdef_dependency_tree_with_lock(state = state)
  for name in names:
    node = tree.get(name)
    if node is None:
      raise VmdefNotFoundError(name)
    dependent_vmdefs = sorted(set(node['vmdefs']) - name_set)
    dependent_vms = sorted(node['vms'])
    if dependent_vmdefs or dependent_vms:
      if dependent_vmdefs and dependent_vms:
        e = 'VMDEF(s) {} and VM(s) {}'.format(dependent_vmdefs,
                                              dependent_vms)
      elif dependent_vmdefs:
        e = 'VMDEF(s) {}'.format(dependent_vmdefs)
      else:
        e = 'VM(s) {}'.format(dependent_vms)
      raise ValueError('Cannot remove VMDEF {} which is required by {}'
                        .format(repr(name), e))

def uninstall_vmdefs(name = None, names = None, force = False):
  names = ([name] if name else []) + (names if names else [])
  install_dir = _get_installed_vmdef_dir()
  install_paths = [os.path.join(install_dir, name + '.vmdef')
                   for name in names]
  if force:
    for path in install_paths:
      os.remove(path)
  else:
    acquire_lock()
    try:
      _verify_vmdefs_are_not_dependencies_with_lock(names)
      for path in install_paths:
        os.remove(path)
    finally:
      release_lock()
  try:
    os.rmdir(install_dir)
  except OSError:
    pass

def register_vmdefs(path = None, paths = None, force = False):
  paths = ([path] if path else []) + (paths if paths else [])
  _, name_cache = _validate_vmdefs_paths(paths)
  acquire_lock()
  try:
    state = load_state()
    vmdefs = state.setdefault('vmdefs', {})
    explicitly_registered = set(state.get('explicitly_registered_vmdefs', []))
    old_len = len(explicitly_registered)
    changed = False
    for path, name in name_cache.items():
      old_path = vmdefs.get(name)
      if old_path != path:
        if not force:
          raise ValueError('VMDEF {} already registered')
        vmdefs[name] = path
        changed = True
      explicitly_registered.add(name)
    if changed or len(explicitly_registered) != old_len:
      if explicitly_registered:
        state['explicitly_registered_vmdefs'] = list(explicitly_registered_vmdefs)
      write_state(state)
  finally:
    release_lock()

def unregister_vmdefs(name = None, names = None, force = False):
  names = ([name] if name else []) + (names if names else [])
  acquire_lock()
  try:
    state = load_state()
    if not force:
      _verify_vmdefs_are_not_dependencies_with_lock(names, state = state)
    vmdefs = state.setdefault('vmdefs', {})
    len_defs = len(vmdefs)
    explicitly_registered = set(state.get('explicitly_registered_vmdefs', []))
    len_exp = len(explicitly_registered)
    for name in names:
      for pop in (vmdefs.pop, explicitly_registered.remove):
        try:
          pop(name)
        except KeyError:
          if not force:
            raise VmdefNotFoundError(name)
    if len(vmdefs) != len_defs or len(explicitly_registered) != len_exp:
      if not vmdefs:
        try:
          state.pop('vmdefs')
        except KeyError:
          pass
      if explicitly_registered:
        state['explicitly_registered_vmdefs'] = list(explicitly_registered)
      else:
        try:
          state.pop('explicitly_registered_vmdefs')
        except KeyError:
          pass
      write_state(state)
  finally:
    release_lock()

def die(message,
        stream = sys.stderr,
        flush = True,
        exception = None,
        return_code = 1,
        name = None):
  stream.write(str(message).strip() + '\n')
  if flush:
    stream.flush()
  raise (exception if exception else SystemExit(return_code))

def format_list(lis):
  fmt = ', '.join(map(shlex.quote, sorted(lis)))
  if fmt:
    import textwrap
    fmt = textwrap.fill(fmt, initial_indent = '  ', subsequent_indent = '  ')
  return fmt

def _do_build_command(vm_name, vmdef_name, command_args, custom_parameters):
  return build(name = vm_name,
               vmdef_name = vmdef_name,
               custom_parameters = custom_parameters)

def _do_boot_command(vm_name, vmdef_name, command_args, custom_parameters):
  return ensure_booted(name = vm_name,
                       vmdef_name = vmdef_name,
                       custom_parameters = custom_parameters)

def _do_stop_command(vm_name, vmdef_name, command_args, custom_parameters):
  return stop(name = vm_name, custom_parameters = custom_parameters)

def _do_rm_command(vm_name, vmdef_name, command_args, custom_parameters):
  return rm(name = vm_name, custom_parameters = custom_parameters)

def _do_sh_command(vm_name, vmdef_name, command_args, custom_parameters):
  ensure_booted(name = vm_name,
                vmdef_name = vmdef_name,
                custom_parameters = custom_parameters)
  return basic_shell(name = vm_name)

def _do_run_command(vm_name, vmdef_name, command_args, custom_parameters):
  ensure_booted(name = vm_name,
                vmdef_name = vmdef_name,
                custom_parameters = custom_parameters)
  c = shlex.join(command_args)
  if sys.stdin.isatty():
    sys.exit(realtime_shell(command = c, name = vm_name))
  output, ret = pipe_string(sys.stdin.buffer.read(), c, name = vm_name)
  sys.stdout.buffer.write(output)
  sys.stdout.buffer.flush()
  sys.exit(ret)

def _do_put_command(vm_name, vmdef_name, command_args, custom_parameters):
  if len(command_args) != 2:
    die('Expected two arguments, got: {}'.format(command_args))
  ensure_booted(name = vm_name,
                vmdef_name = vmdef_name,
                custom_parameters = custom_parameters)
  return put_file(command_args[0], command_args[1], name = vm_name)

def _do_get_command(vm_name, vmdef_name, command_args, custom_parameters):
  if len(command_args) != 2:
    die('Expected two arguments, got: {}'.format(command_args))
  ensure_booted(name = vm_name,
                vmdef_name = vmdef_name,
                custom_parameters = custom_parameters)
  return get_file(command_args[0], command_args[1], name = vm_name)

def _do_generate_id_command(vm_name,
                            vmdef_name,
                            command_args,
                            custom_parameters):
  return print(generate_id())

def _do_ps_command(vm_name, vmdef_name, command_args, custom_parameters):
  # TODO improve details, formatting and arg support, cleanup
  acquire_lock()
  state = load_state()
  release_lock()
  for label, lis in (('Running VMs', list_running_vms()),
                     ('VMs', list_all_vms(state = state)),
                     ('Registered VMDEFs', list_registered_vmdefs(state = state)),
                     ('Installed VMDEFs', list_installed_vmdefs()),
                     ('Well Known VMDEFs', list_well_known_vmdefs())):
    fmt = format_list(lis)
    if fmt:
      print('{}:\n{}\n'.format(label, fmt))
    else:
      sp = label.split()
      label = ' '.join(list(map(str.lower, sp[:-1])) + [sp[-1]])
      print('No {}\n'.format(label))

def _do_call_command(vm_name, vmdef_name, command_args, custom_parameters):
  func = command_args[0]
  if (func != func.lower() or 
      not validate_name(func, raise_exception_if_invalid = False)):
    raise ValueError('Invalid function name: ' + func)
  vmdef = _get_vmdef_for_vm(vm_name, None)
  if not vmdef:
    raise VmdefNotFoundError('No vmdef for {}'.format(repr(vm_name)))
  prox = VMProxy(vm_name, custom_parameters = custom_parameters)
  res = getattr(vmdef, func.upper())(prox, *command_args[1:])
  sys.exit(res)

def _do_update():
  vprint(1, 'Checking for updates...')
  # req = ...
  # resp = ...
  # manifest = json.loads(resp)
  # if js.get('version') == _VERSION
  vprint(1, 'Already on the latest version!')
  return _get_sidecar_vmdef().do_update(manifest, __file__)

def _do_system_command(vm_name, vmdef_name, command_args, custom_parameters):
  # TODO rework this
  sub_command = (vmdef_name if vmdef_name else
                (command_args[0] if command_args else None))
  if sub_command == 'stop':
    return _rpc_call({
      'action': 'stop_daemon',
      'force': _private.get('force_command'),
    })
  # TODO update prune download install uninstall
  die('Invalid sub-command: {}'.format(repr(sub_command)))

def _do_export_command(vm_name, vmdef_name, command_args, custom_parameters):
  return export(name = vm_name, output_path = _private.get('output_path'))

def _get_raw_args(vm_name, vmdef_name, command_args):
  return (
    ([vm_name] if vm_name else []) +
    ([vmdef_name] if (vmdef_name and vmdef_name != vm_name) else []) +
    command_args
  )

def _do_import_command(vm_name, vmdef_name, command_args, custom_parameters):
  res = import_vms(paths = _get_raw_args(vm_name, vmdef_name, command_args),
                   replace_vms = _private.get('force_command'),
                   replace_vmdefs = _private.get('force_command'),
                   dry_run = _private.get('dry_run'),
                   custom_parameters = custom_parameters)
  if type(res) is str:
    print(res)
  return res

def _do_install_command(vm_name, vmdef_name, command_args, custom_parameters):
  return install_vmdefs(paths = _get_raw_args(vm_name, vmdef_name, command_args),
                        replace_vmdefs = _private.get('force_command'))

def _do_uninstall_command(vm_name, vmdef_name, command_args, custom_parameters):
  return uninstall_vmdefs(names = _get_raw_args(vm_name, vmdef_name, command_args),
                          force = _private.get('force_command'))

def _do_register_command(vm_name, vmdef_name, command_args, custom_parameters):
  return register_vmdefs(paths = _get_raw_args(vm_name, vmdef_name, command_args),
                         force = _private.get('force_command'))

def _do_unregister_command(vm_name, vmdef_name, command_args, custom_parameters):
  return unregister_vmdefs(names = _get_raw_args(vm_name, vmdef_name, command_args),
                           force = _private.get('force_command'))


def handle_args(vmdef_name = None):
  if 'virtuator' not in sys.modules:
    module = types.ModuleType('virtuator')
    module.__dict__.update(inspect.currentframe().f_globals)
    sys.modules['virtuator'] = module
  command = None
  current_option = None
  vm_name = None
  command_args = []
  always_command_args = False
  custom_parameters = {}
  argv = list(reversed(sys.argv[1:]))
  while len(argv) > 0:
    arg = argv.pop()
    if always_command_args:
      command_args.append(arg)
    elif arg == '--':
      always_command_args = True
    elif arg in ('--name', '-n'):
      try:
        vm_name = argv.pop()
      except IndexError:
        die('Too few arguments for {}'.format(arg))
    elif arg in ('--port', '-p'):
      try:
        protocol = argv.pop().lower()
        if protocol not in ('tcp', 'udp'):
          die('Unsupported protocol: {}'.format(repr(protocol)))
        host_port = int(argv.pop())
        guest_port = int(argv.pop())
        _private.setdefault('ports', []).append({
          'protocol': protocol,
          'host_port': host_port,
          'guest_port': guest_port,
        })
      except IndexError:
        die('Too few arguments for {}'.format(arg))
    elif arg in ('--define', '-d'):
      try:
        k = argv.pop()
        v = argv.pop()
        custom_parameters[k] = v
      except IndexError:
        die('Too few arguments for {}'.format(arg))
    elif arg in ('--output', '-o'):
      try:
        _private['output_path'] = argv.pop()
      except IndexError:
        die('Too few arguments for {}'.format(arg))
    elif arg in ('--graphics', '-g'):
      _private['force_graphics'] = True
    elif arg in ('--force', '-f'):
      _private['force_command'] = True
    elif arg in ('--dry-run', '-D'):
      _private['dry_run'] = True
    elif arg in ('--help', '-h'):
      return print_help()
    elif arg in ('--write-html', '-w'):
      _private['write_html'] = True
    elif arg[0:1] == '-' and arg[1:2] != '-':
      for a in arg[1:]:
        if a in 'fghwD':
          argv.append('-' + a)
        else:
          die('Invalid option: -{}'.format(a))
    elif arg.startswith('--') and command is None:
      die('Invalid option: {}'.format(arg))
    elif command is None:
      command = arg
    elif vmdef_name is None:
      vmdef_name = arg
    else:
      command_args.append(arg)
  if not command:
    return print_help()
  if current_option is not None:
    die('No value for option: {}'.format(current_option))
  if vm_name is None:
    vm_name = vmdef_name
  command_func = globals().get('_do_{}_command'.format(command.lower()))
  if type(command_func) is type(_C):
    return command_func(vm_name, vmdef_name, command_args, custom_parameters)
  if _which('virtuator') == __file__:
    help_cmd = repr('virtuator --help')
  else:
    help_cmd = repr(shlex.join((sys.executable, __file__, '--help')))
  die('Invalid command: {}\nUse {} for help'.format(repr(command), help_cmd))

# TODO:
# virtuator system install
# virtuator system update
# virtuator system download os
# virtuator install /path/to/some.vmdef
# virtuator uninstall some_vmdef_name (prompt if no --force)
# virtuator register /path/to/some.vmdef
# virtuator unregister some_vmdef_name (alias to uninstall, track installed vs registered)
# virtuator export some_vm  (makes $PWD/some_vm.vm.tar.gz)
# virtuator export some_vm  --output /some/existing/dir (makes /some/existing/dir/some_vm.vm.tar.gz)
# virtuator export some_vm  --output /some/path (write the vm to /some/path)
# virtuator import some_vm.vm.tar.gz

  # TODO other commands: list args, download all|fw|os|wk, call <name> <func>, run <name> <command> -- <args>, prune --all, pipesh, register
  # TODO args: unique_name, --rm, -s/share, -j --json, --shutdown, -v/--verbose/--verbosity, -q/--quiet, -s/--script, -g/--graphics

def _get_last_vmdef_dict_from_stack():
  frame = inspect.currentframe()
  lib_globals = frame.f_globals
  while frame is not None:
    globs = frame.f_globals
    if globs is not lib_globals:
      return globs
    frame = frame.f_back
  return None

def _query_vmdef_dict(vmdef_name = None, vmdef_dict = None, vmdef = None):
  if vmdef_dict:
    return vmdef_dict
  if vmdef:
    return vmdef.__dict__
  if vmdef_name:
    vmdef = vmdef_from(vmdef_name, require_build = False)
    if vmdef:
      return vmdef.__dict__
  vmdef_dict = _get_last_vmdef_dict_from_stack()
  if not vmdef_dict:
    raise VmdefNotFoundError(vmdef_name)
  return vmdef_dict

def super(vmdef_name = None,
          vmdef_dict = None,
          vmdef = None,
          index = None,
          all = False,
          name = None):
  this_vmdef_dict = _query_vmdef_dict(vmdef_name = vmdef_name,
                                      vmdef_dict = vmdef_dict,
                                      vmdef = vmdef)
  req = this_vmdef_dict.get('REQUIRES')
  if type(req) is str:
    req = (req,)
  if req is None:
    req = ()
  if not all and len(req) < 1:
    raise ValueError('REQUIRES is unset for {} at {}'.format(
      shlex.quote(this_vmdef_dict.get('NAME')),
      shlex.quote(this_vmdef_dict.get('__file__'))))
  if all:
    super_vmdefs = []
    for super_name in req:
      super_vmdef = vmdef_from(super_name)
      if not super_vmdef:
        raise VmdefNotFoundError(super_name)
      super_vmdefs.append(super_vmdef)
    return super_vmdefs
  if index is None:
    super_name = name if name in req else req[0]
  else:
    super_name = req[index]
  super_vmdef = vmdef_from(super_name)
  if not super_vmdef:
    raise VmdefNotFoundError(super_name)
  return super_vmdef

def inherit(vmdef_name = None,
            vmdef_dict = None,
            vmdef = None,
            index = None,
            all = False,
            name = None):
  this_vmdef_dict = _query_vmdef_dict(vmdef_name = vmdef_name,
                                      vmdef_dict = vmdef_dict,
                                      vmdef = vmdef)
  res = super(vmdef_dict = this_vmdef_dict, index = index, all = all)
  super_vmdefs = res if type(res) is list else (res,)
  for super_vmdef in super_vmdefs:
    this_vmdef_dict.update(super_vmdef.__dict__ | this_vmdef_dict)

def inherit_all(vmdef_name = None,
                vmdef_dict = None,
                vmdef = None,
                name = None):
  inherit(vmdef_name = vmdef_name,
          vmdef_dict = vmdef_dict,
          vmdef = vmdef,
          all = True)

def handle(auto_inherit = True):
  vmdef_dict = _get_last_vmdef_dict_from_stack()
  if auto_inherit:
    inherit_all(vmdef_dict = vmdef_dict)
  validate_vmdef(vmdef_dict)
  if vmdef_dict.get('__name__') == '__main__':
    vmdef = types.ModuleType(vmdef_dict.get('NAME'))
    vmdef.__dict__.update(vmdef_dict)
    _private.setdefault('vmdefs', {})[vmdef.NAME] = vmdef
    sys.exit(handle_args(vmdef_name = vmdef.NAME))

def _list_running_with_lock():
  _cleanup_dead_vms_and_find_vm_with_lock()
  vms = _private.get('vms')
  return list(vms.keys()) if vms else []

def _list_running_internal():
  with _private['lock']:
    return _list_running_with_lock()

def _wait_until_stopped_internal(payload = None, name = None):
  name = payload['name'] if payload else name
  proc = None
  with _private['lock']:
    try:
      proc = _private['vms'][name]['proc']
    except KeyError:
      pass
  if proc:
    try:
      proc.wait(timeout = (payload.get('timeout') if payload else None))
    except subprocess.TimeoutExpired:
      pass

def _wait_until_rpc_idle():
  while True:
    idle_time = time.time() - _private['last_request_time']
    is_idle = (idle_time >= _C('rpc_idle_timeout'))
    if is_idle:
      return
    time.sleep(_C('rpc_idle_timeout') - idle_time)

def _stop_daemon_common(force, no_new_vms, wait_idle):
  if no_new_vms:
    _private['no_new_vms'] = True
  if not force:
    while True:
      if wait_idle:
        _wait_until_rpc_idle()
      with _private['lock']:
        running_vms = _list_running_with_lock()
        if not running_vms and not no_new_vms:
          _private['no_new_vms'] = True
      for name in running_vms:
        _wait_until_stopped_internal(name = name)
      if not running_vms:
        break
  if is_unix:
    try:
      os.kill(os.getpid(), signal.SIGINT)
    except KeyboardInterrupt:
      pass
  else:
    with _private['lock']:
      _private.setdefault('daemon_stop_threads', []).append(
        threading.current_thread()
      )
    stop_rpc_loop_from_external_thread()

def _stop_daemon_internal(payload):
  return _stop_daemon_common(payload.get('force'), True, False)

def handle_rpc_request_payload(payload):
  action = payload.get('action')
  if action == 'boot':
    return _boot_internal(payload)
  elif action == 'send_keys':
    return _send_keys_internal(payload)
  elif action == 'read_output':
    return _read_output_internal(payload)
  elif action == 'list_running':
    return _list_running_internal()
  elif action == 'stop_vm':
    return _stop_internal(payload.get('name'))
  elif action == 'wait_until_vm_stopped':
    return _wait_until_stopped_internal(payload = payload)
  elif action == 'stop_daemon':
    return _stop_daemon_internal(payload = payload)
  else:
    raise AttributeError('no RPC action for {}'.format(repr(action)))

def handle_new_rpc_request(next_request):
  if is_unix:
    connection, addr = next_request
    try:
      payload = try_parse_payload(socket_readline(connection))
    except (json.JSONDecodeError, ConnectionResetError, BrokenPipeError):
      return
  else:
    payload = try_parse_payload(next_request)
    if not payload:
      return
    pid = payload.get('pid')
    if not pid:
      return

  rpayload = {'error': None, 'result': None}
  try:
    rpayload['result'] = handle_rpc_request_payload(payload)
  except Exception as exc:
    rpayload['error'] = serialize_exception(exc)

  try:
    if is_unix:
      connection.send((json.dumps(rpayload)+'\n').encode())
    else:
      cls = '{}{}'.format(RPC_CLIENT_CLASS_PREFIX, payload['pid'])
      hwnd = find_rpc_receiver(cls)
      if hwnd:
        rpayload['id'] = payload['id']
        send_rpc_payload(hwnd, rpayload)
  except (ConnectionResetError, BrokenPipeError):
    pass

def _handle_new_rpc_request_async(next_request):
  if not next_request:
    return
  _private['last_request_time'] = time.time()
  threading.Thread(
    target = handle_new_rpc_request,
    args = (next_request,),
    daemon = True,
  ).start()

def daemon_timeout_thread():
  _stop_daemon_common(False, False, True)

def daemon_main():
  _private['last_request_time'] = time.time()
  if is_unix:
    sock_path = get_socket_path()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(sock_path)
  else:
    ensure_rpc_result_receiver_ready(None)
  threading.Thread(target = daemon_timeout_thread, daemon = True).start()
  if is_unix:
    try:
      while True:
        server.listen(1)
        _handle_new_rpc_request_async(server.accept())
    except KeyboardInterrupt:
      pass
    finally:
      if is_unix:
        try:
          os.remove(sock_path)
        except FileNotFoundError:
          pass
  else:
    do_rpc_loop(_handle_new_rpc_request_async)
    with _private['lock']:
      for thread in _private.get('daemon_stop_threads', []):
        thread.join()

def main():
  try:
    with open('/proc/self/comm', 'r+') as f:
      f.write('virtuator')
  except (FileNotFoundError, PermissionError):
    pass

  if is_daemon():
    daemon_main()
  else:
    handle_args()

if __name__ == '__main__':
  main()


# TODO import/export, install/uninstall, search vmdefs dir even if not installed
# TODO virtuator system install --system --user --bin --no-bin --portable (--script aware)
# TODO virtuator stop --force somevm, virtuator stop -f somevm
# TODO gpg signed self-updates, version number in URL e.g. /metadata/updates.r1.toml
# TODO if no gpg, use VM to download and verify update
# TODO w/o non-interactive flag, prompt re firmware download

# TODO better output streaming

# TODO help command via python -m pydoc virtuator
# TODO call? function which calls virtuator with args
# TODO status command to list backends, data dir, etc

# TODO cmdline arg to boot with systemimg even after built
# TODO rescue from efi shell if stuck

# TODO cpu count support

# TODO --arch arg to force arch
# TODO boot time, prune old/unused vms

# TODO glob vmnames and iter command_funs
# TODO ps return indicates if vm exists, --type arg
# TODO --vmname as alias for name?

# TODO custom_backend_args support - filter out redundant arg within make_full_backend_cmd

# TODO virtuator system deptree alpine
