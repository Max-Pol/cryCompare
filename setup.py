from setuptools import setup

setup(name='cryptoscrap',
      version='0.1',
      description='CryptoCompare Web Scraper',
      url='https://github.com/Max-Pol/cryptoscrap',
      author='Max-Pol',
      license='MIT',
      packages=['cryptoscrap'],
      install_requires=[
          'requests',
          'pandas',
      ],
      zip_safe=False)
