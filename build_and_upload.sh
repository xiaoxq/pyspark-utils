#!/usr/bin/env bash

pushd "$( dirname "${BASH_SOURCE[0]}" )"
  # Source code.
  python setup.py sdist
  # Python2 wheel.
  python2 setup.py bdist_wheel
  # Python3 wheel.
  python3 setup.py bdist_wheel
  # Upload all.
  python3 -m twine upload dist/*
popd
