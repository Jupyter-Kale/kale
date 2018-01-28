import parsl

def parsl_wrap(fn, dfk, *args, **kwargs):
    """Wrapper to generate Parsl dependencies.

    Args:
        fn: function to be wrapper
        depends: list of Parsl apps (or wrapped functions) which must execute first
        dfk: Parsl DataFlowKernel
        *args: args for fn
        **kwargs: kwargs for fn


    """

    @parsl.App('python', dfk)
    def wrapper(*depends):
        return fn(*args, **kwargs)

    # Preserve name of function
    wrapper.__name__ = fn.__name__

    return wrapper

def parsl_app_after_futures(app, futures, dfk):
    """Execute fn after deps

    app is a parsl app which returns a future
    futures is a list of Parsl futures which must complete before execution
    dfk is the Parsl DataFlowKernel

    """

    @parsl.App('python', dfk)
    def wrapper(*depends):
        return app()

    # Preserve name of app
    wrapper.__name__ = app.__name__

    return wrapper(*futures)
