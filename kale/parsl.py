from parsl import App

def parsl_func_after_futures(fn, futures, dfk, *args, **kwargs):
    """Execute fn after deps

    fn is a parsl function which returns a future
    futures is a list of Parsl futures
    dfk is the Parsl DataFlowKernel

    """

    @App('python', dfk)
    def wrapper(*futures):
        return fn(*args, **kwargs)

    return wrapper(*futures).result()

def parsl_wrap(fn, dfk, *args, **kwargs):
    """Wrapper to generate Parsl dependencies.

    Args:
        fn: function to be wrapper
        depends: list of Parsl apps (or wrapped functions) which must execute first
        dfk: Parsl DataFlowKernel
        *args: args for fn
        **kwargs: kwargs for fn


    """

    @App('python', dfk)
    def wrapper(*depends):
        return fn(*args, **kwargs)

    return wrapper
