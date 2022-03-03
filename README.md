[![tests](https://github.com/JGoutin/repoup/actions/workflows/tests.yml/badge.svg)](https://github.com/JGoutin/repoup/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/JGoutin/repoup/branch/main/graph/badge.svg?token=xTt9tdHPwH)](https://codecov.io/gh/JGoutin/repoup)
[![PyPI](https://img.shields.io/pypi/v/repoup.svg)](https://pypi.org/project/repoup)

Repoup is a utility to manage packages repositories stored on the cloud from serverless
functions. For instance, it can manage an RPM repository stored on S3 from an AWS lambda
function.

Features:

 * Repository initialization.
 * Packages addition and deletion.
 * Packages signature.
 * Multiple repository management with a single instance.
 * Autodetection of the repository to update based on the package name information 
   (Architecture, OS version, package format).

Supported repositories:

 * RPM: `.rpm` packages repositories used with DNF/YUM Linux package managers.

Supported cloud:

 * AWS: AWS S3 (or compatible) as storage with optional Cloudfront CDN. 
   AWS Lambda as repository updater.

# Installation

The package is available on PyPI and can be installed using Pip:

```bash
pip install repoup
```

By default, only the minimum dependencies are installed. It is possible to install
more dependencies using the following extras:

* `speedups`: Optional libraries to improve performance.
* `rpm`: RPM repositories support.
* `aws`: AWS cloud provider support.

For instance:

```bash
pip install repoup[rpm,speedups]
```

## Packages and metadata signature extra requirements:

The `gnupg` (Version 2) command is required.

GPG keys with passwords also requires that the `gpg-agent` is not already started or is 
started with the `--allow-preset-passphrase` parameter.

### RPM packages

To sign RPM package, the `rpm-sign` package is required.

#### Signature verification

If the repository is called with `gpg_verify=True` to activate signature verification
after signing, the `rpm` command require to be run as Root. This is required to add 
and remove the public key to the keyring.

It is possible to set the `RPM_GPG_REQUIRE_SUDO=1` environment variable to 
automatically use `sudo`. This requires that `sudo` does not require password.
