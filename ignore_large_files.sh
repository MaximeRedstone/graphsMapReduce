#!/bin/bash

sed '/# Large files/,$d' < .gitignore > tmp
rm .gitignore
mv tmp .gitignore
echo '# Large files' >> .gitignore
find . -not -path '*/\.*' -size +50M | cut -c 3- >> .gitignore
