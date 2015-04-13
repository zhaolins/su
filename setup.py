import os

from setuptools import setup, find_packages

use_cython = False
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.txt')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.txt')) as f:
    CHANGES = f.read()

requires = [
    'pyramid',
    'pyramid_mako',
    'pyramid_debugtoolbar',
    'pyramid_tm',
    'pytz',
    # 'pika',
    # 'urllib.parse',
    'SQLAlchemy',
    'transaction',
    'waitress',
    'pyyaml',
    'python3-memcached',
    'psycopg2',
    'py-bcrypt',
    'redis',
    'hiredis',
    'pyramid_beaker',
]

if use_cython:
    from Cython.Build import cythonize
    import fnmatch

    pyx_modules = []
    for root, dirs, files in os.walk('.'):
        for f in fnmatch.filter(files, '*.pyx'):
            pyx_modules.append(os.path.join(root, f))
    pyx_extensions = cythonize(pyx_modules)
else:
    pyx_extensions = []


setup(name='SU',
      version='0.0',
      description='SU',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
        "Programming Language :: Python",
        "Framework :: Pyramid",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
      ],
      author='zhaolin.su',
      author_email='',
      url='',
      keywords='web pyramid pylons',
      #packages=find_packages(),
      packages = find_packages('src'),
      package_dir = {'': 'src'},
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=requires,
      test_suite="su.tests",
      ext_modules=pyx_extensions,
      entry_points="""\
      [paste.app_factory]
      main = su:main
      """,
      )
