from distutils.core import setup

setup(
    name='kale_workflows',
    version='0.1',
    license='BSD',
    description='Jupyter HPC Workflows',
    packages={'kale'},
    install_requires=[
        'bqplot>=0.10',
        'fireworks>=1.4',
        'ipython>=6.1.0',
        'ipyvolume>=0.3.2',
        'ipywidgets>=6.0',
        'matplotlib>=2.0',
        'networkx>=1.11',
        'numpy>=1.12',
        'pandas>=0.20.3',
        'paramiko>=2.3.0',
        'pydot>=1.2.3',
        'traitlets>=4.3.2'
    ]
)
