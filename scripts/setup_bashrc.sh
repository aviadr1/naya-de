#!/bin/sh

# https://stackoverflow.com/a/60559856
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )";
# parent dir of that dir
ROOT_DIRECTORY="${DIR%/*}"

echo this will overwrite your .bashrc 
cp $DIR/.bashrc ~/.bashrc
