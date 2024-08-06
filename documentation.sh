#! /bin/bash
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

echo -e "Generating Doxygen..."
rm -rf docs/
doxygen Doxyfile > /dev/null 2>&1
mv docs/html/* docs/
rmdir docs/html
echo -e "${GREEN}Build successful.${NC}"