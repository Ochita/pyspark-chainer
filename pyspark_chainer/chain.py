from functools import wraps
from inspect import getmembers, isfunction

from pyspark.sql import DataFrame


# TODO typings
# TODO logging
# TODO moar checks
# TODO add lineage mechanisms
class Chain(object):
    def __init__(self, /, df, module=None, transformations=None):
        def predicate(member):
            return isfunction(member) and member.__module__ == module.__name__
        self.df = df
        if transformations:
            self.transformations = transformations
        elif module:
            self.transformations = dict(getmembers(module, predicate))
            # TODO  module function check signature starts with DataFrame
            for tf in self.transformations.keys():
                if hasattr(df, tf):
                    raise AttributeError(f'Transformation {tf} '
                                         f'match with DataFrame attribute')
        else:
            self.transformations = dict()

    def wrap_df_operation(self, fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            result = fun(*args, **kwargs)
            if isinstance(result, DataFrame):
                return Chain(result, transformations=self.transformations)
            return result
        return wrap

    def wrap_transformation(self, fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            result = fun(self.df, *args, **kwargs)
            if isinstance(result, DataFrame):
                return Chain(result, transformations=self.transformations)
            return result
        return wrap

    def __getattr__(self, name):
        attr = None
        if hasattr(self.df, name):
            attr = getattr(self.df, name)
            if callable(attr):
                return self.wrap_df_operation(attr)
        if name in self.transformations:
            if attr:
                raise AttributeError(f'Attribute {name} is ambitious')
            attr = self.transformations[name]
            return self.wrap_transformation(attr)
        if not attr:
            raise AttributeError(f'Attribute {name} not exists')
        return attr
