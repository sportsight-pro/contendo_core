import os
import logging.config
from pathlib import Path
from functools import wraps

import yaml

def contendo_logging_setup(
    default_path='{}/contendo_logging.yaml'.format(Path(__file__).parent),
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

def contendo_function_logger(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        func.__globals__['logger'] = logging.getLogger(func.__name__)
        return func(*args, **kwargs)
    return wrapper

def contendo_classfunction_logger(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        func.__globals__['logger'] = logging.getLogger('{}.{}'.format(type(self).__name__, func.__name__))
        return func(self, *args, **kwargs)
    return wrapper


def inject_variables(context):
    """ Decorator factory. """
    def variable_injector(func):
        @wraps(func)
        def decorator(*args, **kwargs):
            try:
                func_globals = func.__globals__  # Python 2.6+
            except AttributeError:
                func_globals = func.func_globals  # Earlier versions.

            saved_values = func_globals.copy()  # Shallow copy of dict.
            func_globals.update(context)

            try:
                result = func(*args, **kwargs)
            finally:
                func_globals = saved_values  # Undo changes.

            return result

        return decorator

    return variable_injector

if __name__ == '__main__':
    namespace = {'a': 5, 'b': 3}

    @inject_variables(namespace)
    def test():
        print('a:', a)
        print('b:', b)

    test()
