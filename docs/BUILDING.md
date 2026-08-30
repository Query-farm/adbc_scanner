# Building the extension

See the [development documentation](../CLAUDE.md) for build instructions.

```bash
# Clone with submodules
git clone --recurse-submodules git@github.com:Query-farm/adbc-scanner.git

# Set up vcpkg
export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build
GEN=ninja make

# Test
make test
```
