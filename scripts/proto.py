#!/usr/bin/python

from os import getcwd, pathsep, defpath, environ, walk, system
from os.path import exists, realpath, join, dirname
from subprocess import check_output
import fnmatch
import re

# presume schemas is a sibling of this codebase
# make use of the extra "proto" directory that CH has put in there
script_path = dirname(realpath(__file__))
server = realpath(join(script_path, ".."))
schemas = join(server, "../schemas")

if not exists(schemas):
    raise Exception, "Can't find schemas folder. Assumed it would be at %s" % realpath(schemas)

src = realpath(join(schemas, "src/main/resources"))

if not exists(join(server, "proto")):
    raise Exception, "Can't find destination python directory %s" % realpath(join(server, "proto"))

if not exists(src):
    raise Exception, "Can't find source proto directory %s" % realpath(src)

def find_in_path(cmd):
    PATH = environ.get("PATH", defpath).split(pathsep)
    for x in PATH:
        possible = join(x, cmd)
        if exists(possible):
            return possible
    return None

# From http://stackoverflow.com/a/1714190/320546
def version_compare(version1, version2):
    def normalize(v):
        return [int(x) for x in re.sub(r'(\.0+)*$','', v).split(".")]
    return cmp(normalize(version1), normalize(version2))

protocs = [realpath(x) for x in "%s/protobuf/src/protoc" % server, find_in_path("protoc") if x != None]
protoc = None
for c in protocs:
    if not exists(c):
        continue
    output = check_output([c, "--version"]).strip()
    try:
        (lib, version) = output.split(" ")
        if lib != "libprotoc":
            raise Exception
        if version_compare("3.0.0", version) > 0:
            raise Exception
        protoc = c
        break

    except Exception,e :
        print "Not using %s because it returned '%s' rather than \"libprotoc <version>\", where <version> >= 3.0.0" % (c, output)

if protoc == None:
    raise Exception, "Can't find a good protoc. Tried %s" % protocs
print "Using %s for protoc" % protoc

protos = []
for root, dirs, files in walk(src):
    protos.extend([join(root, f) for f in fnmatch.filter(files, "*.proto")])
if len(protos) == 0:
    raise Exception, "Didn't find any proto files in %s" % src
cmd = "%s -I %s --python_out=%s %s" % (protoc, src, server, " ".join(protos))
system(cmd)

for root, dirs, files in walk(join(server, "proto")):
    if "__init__.py" not in files:
        print "Creating __init__.py in %s" % root
        file(join(root, "__init__.py"), "w").close()
