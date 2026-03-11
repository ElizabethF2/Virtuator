# Virtuator

Virtuator is a small, easy to use, portable, rootless virtual machine manager designed to consistently build VM images and run scripts in those VMs. Virtuator's only dependencies are Qemu and Python. It does not use any other libraries and no packages outside of the standard Python packages are used. Virtuator is designed as a lightweight alternative to Docker or Vagrant which can be used in restrictive environments or uncommon operating systems where those tools can't be used, such as in Termux on Android, while also supporting scenarios which require full virtualization, such as testing kernel changes. Virtuator can be run without installation and without needing to be made executable. Virtuator will use hardware accelerated virtualization if your OS supports it and if your account has permission to use it, however, if not, it will fallback to pure software virtualization. As long as your host machine has Qemu and Python, it can run Virtuator.

Virtuator includes VM definitions for several common operating systems. To build an Arch Linux VM, boot it and open a shell on it, you can simply run:

````
virtuator sh archlinux
````

Virtuator can run on any host architecture and guest VMs can used x64, x86, arm64, arm and RISC-V. Support for other guest architectures is planned. See the Roadmap section below.

Virtuator VMs are defined using virtual machine definitions (vmdefs) which are Python scripts similar to Dockerfiles or Vagrantfiles. These scripts follow a schema which includes a BUILD functions which define how the VM is built such that running build on the same vmdef on two different machines will produce the same VM regardless of the host VM. Vmdefs can also include user-defined custom functions, for example COMPILE and TEST functions could be defined then invoked via `virtuator run <vmname> compile` and `virtuator run <vmname> test` to compile and testing code within a VM. New VMs can quickly and easily be defined by extending the definitions included with Virtuator.

Virtuator also includes two utilities which use it. The first, virtuator_wrapshell, can automatically create, boot and open a shell in a VM with the user's local files shared with the VM so that programs can be used within the VM that would not otherwise be possible while still having access to existing programs and files. The second, virtuator_dockerc, will attempt to convert a Dockerfile to a virtuator VM definition and can be used to quickly port containers to Virtuator.

## Installation

Before installing Virtuator, first ensure that you have Python and Qemu installed. The exact steps for installing both will vary based on your OS but some examples are provided below. As noted above, Python and Qemu are required to use Virtuator so you will not be able to use it if you do not already have both installed and you are unable to install them.

Alpine Linux: `apk add --no-cache python3 qemu-system-x86_64`
Arch Linux (including Manjaro, EndeavourOS, SteamOS, etc): `pacman -Syu python qemu-system-x86 qemu-img edk2-ovmf`
Debian (Ubuntu, Mint, Pop!_OS, etc): `apt install python3 qemu-system-x86`
Fedora: `yum install python qemu-system-x86`
Gentoo: `emerge dev-lang/python app-emulation/qemu`
openSUSE: `zypper install python3 qemu`
Void Linux: `xbps-install -Su python3 qemu`
Termux: `pkg install python3 qemu-system-aarch64-headless ovmf`
Homebrew (OSX, etc): `brew install python qemu`
FreeBSD: `pkg install python3 qemu`
Windows: `winget install python QEMU`

Use the command below to run the install wizard and follow its prompts:
````
python -c "import hashlib,urllib.request;r=urllib.request.urlopen('https://example.org/#virtuator/install.py').read();print(r) if hashlib.sha256(r).hexdigest()=='ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9' else print(bytes.fromhex('426164204861736821').decode())"
````

If you see an error regarding `python` not being found or not being recognized as a command or, on Windows, if the store app opens, try replacing `python` with `python3` or `py`. If you get the error `No module named request`, try running the command again with `python` replaced with `python3` and ensure your version of Python is up to date.

To uninstall, run `virtuator uninstall`.
