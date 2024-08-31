# Shared-code folder: `common`

## TL;DR

> This entire folder hierarchy of `common` is copied into all functions automatically.

## No, I wan't to read the long story

A common use case when you deal with the deployment of multiple functions simultaneously, is that you do not want to
replicate shared code between all the function folders. In order to accomodate this, as part of the workflow
`function-action` this template relies on, we copy all the contents in this folder (can be specified by `common_folder`
in the workflow files, see below) the functions we upload to Cognite Functions. If this is not specified, we check if
`common/` exists in the root folder and if so, _**we use it automatically**_ :rocket:

### Handling imports

A typical setup looks like this:

```bash
├── common
│   └── utils.py
└── my_function
    └── handler.py
```

The code we zip and send off to the FilesAPI will look like this:

```bash
├── common
│   └── utils.py
└── handler.py
```

This means your `handler.py`-file should do imports from `common/utils.py` like this:

```python
from common.utils import my_helper1, my_helper2
import common.utils as utils  # alternative
```

## No, I want `common` to be named `snowflake_utilities`

No problem mate, locate `.github/workflow` and open the following files:

- `deploy-pr.yaml`
- `deploy-push.yaml`

Then, in each file, scroll down until you see:

```yaml

- name: Deploy and schedule ${{ matrix.function }}
  uses: cognitedata/function-action@v2
  with:
    function_name: ...
    common_folder: snowflake_utilities  # <-- add it here
    ...
```
