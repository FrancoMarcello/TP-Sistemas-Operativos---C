# Libraries
LIBS=utils commons pthread readline m crypto

# Custom libraries' paths
SHARED_LIBPATHS=
STATIC_LIBPATHS=../utils

# Compiler flags
CDEBUG=-g -Wall -DDEBUG -fdiagnostics-color=always -Wno-deprecated-declarations
CRELEASE=-O3 -Wall -DNDEBUG -Wno-deprecated-declarations

# Source files (*.c) to be excluded from tests compilation
TEST_EXCLUDE=src/main.c
