from setuptools import Extension, setup
setup(ext_modules=[Extension("tcp", ["tcpmodule.c"])])
