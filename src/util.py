import os


def reraise(e, *args):
    e.args = args + e.args
    raise e.with_traceback(e.__traceback__)


def get_env_var(var_name: str) -> str:
    """Gets an environment variable"""
    try:

        return os.environ[var_name]
    except KeyError as e:
        reraise(f"Can't find {var_name} in env!")
