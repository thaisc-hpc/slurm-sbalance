Verbosity = type('Verbosity', (), {'INFO':1, 'WARNING':2, 'DEBUG':5, 'DEBUG2':6})

class VerboseLog:
    verbose = None

    @classmethod
    def set_verbose(cls, verbose: int):
        if verbose:
            def verbose_print(*a, **k):
                if k.pop('level', 0) <= verbose:
                    print(*a, **k)
        else:
            verbose_print = lambda *a, **k: None
        cls.verbose = verbose_print

    @classmethod
    def print(cls, *a, **k):
        cls.verbose(*a, **k)
