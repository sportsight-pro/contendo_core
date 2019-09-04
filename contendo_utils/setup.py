from setuptools import setup

setup(name='contendo_utils',
      version='0.2.3',
      description='General python and google-cloud related utilities',
      url='https://github.com/sportsight-pro/sportsight-core/tree/master/contendo_utils',
      author='Yahali Sherman, contendo.ai',
      author_email='yahali@contendo.ai',
      license='MIT',
      packages=['contendo_utils'],
      install_requires=[
          'gcsfs',
          'google-cloud-bigquery',
          'google-cloud-storage',
      ],
      zip_safe=False)
