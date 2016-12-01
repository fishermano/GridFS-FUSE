# -*- mode: python; -*-
import os

env = Environment()
env.Append(CPPFLAGS=['-D_FILE_OFFSET_BITS=64'])
env.Append(CPPFLAGS=['-g'])

fuse_lib = "fuse"

if 'darwin' == os.sys.platform:
    env.Append(CPPPATH=['/usr/include/boost'],
               LIBPATH=['/usr/lib/boost'])

    if os.uname()[2] == '10.0.0':
        # Snow Leopard uses 64 bit inodes
        fuse_lib += '_ino64'

conf = Configure( env )
libs = [ "mongoclient" , fuse_lib ]
boostLibs = [ "thread" , "system" , "filesystem" ]

def checkLib( n ):
    if conf.CheckLib( n ):
        return True
    print( "Error: can't find library: " + str( n ) )
    Exit(-1)
    return False

for x in libs:
    checkLib( x )

def makeBoost( x ):
    # this will have to get more complicated later
    return "boost_" + x + "-mt";

#conf.CheckLib(makeBoost("system"))

for x in boostLibs:
    checkLib( makeBoost( x ) )

#conf.CheckLib( makeBoost( "system" ) )

env = conf.Finish()

files = ['main.cpp', 'operations.cpp', 'options.cpp', 'local_gridfile.cpp']

env.Program('mount_gridfs', files)

def runUnitTest(env, target, source):
    import subprocess
    app = str(source[0].abspath)
    if not subprocess.call(app):
        open(str(target[0]), 'w').write("PASSED\n")

test_target = env.Command('tests.passed', 'tests/basic.py', runUnitTest)
env.Alias("test", "tests.passed")
env.Depends(test_target, 'mount_gridfs')

env.Default(['mount_gridfs'])
