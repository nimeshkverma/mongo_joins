from distutils.core import setup

setup(name='mongojoin',
      packages=['mongojoin'],
      version='1.0.1',
      description='Python library for performing joins on MongoDB collections',
      author='Nimesh Kiran (supported ably by DrCricket)',
      author_email='nimesh.aug11@gmail.com',
      url='https://github.com/nimeshkverma/mongo_joins',
      download_url='https://github.com/nimeshkverma/mongo_joins/tarball/1.0.1',
      py_modules=['mongojoin'],
      install_requires=['pymongo'],
      keywords=['mongo', 'joins', 'aggregations'],
      classifiers=[],
      )
