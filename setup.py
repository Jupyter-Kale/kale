from distutils.core import setup

setup(
    name='kale_workflows',
    version='0.1',
    license='BSD',
    description='Jupyter HPC Workflows',
    packages={'kale'},
    install_requires=[
        'bqplot>=0.10',
        'drmaa>=0.7.8',
        'fireworks>=1.6.9',
        'ipython>=6.1.0',
        'ipyvolume>=0.4.0',
        'ipywidgets>=7.2.1',
        'matplotlib>=2.0',
        'networkx>=1.11',
        'numpy>=1.12',
        'pandas>=0.20.3',
        'paramiko>=2.3.0',
        'psutil>=5.3.1',
        'pydot>=1.2.3',
        'qgrid>=1.0.2',
        'requests>=2.18.4',
        'sanic>=0.7.0',
        'traitlets>=4.3.2'
    ]
)
