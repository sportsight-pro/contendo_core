import os
print ('[Register]')
#os.system('python setup.py register')
print ('[sdist]')
os.system('python setup.py sdist')
print ('[Upload-test]')
#os.system('twine upload --repository-url https://test.pypi.org/legacy/ dist/*')
print ('[Upload]')
os.system('twine upload dist/*')
