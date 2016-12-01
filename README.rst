===========
GridFS FUSE
===========

Allows you to mount a MongoDB GridFS instance as a local filesystem.

Requirements
============

* A recent (v1.1.2 or later) MongoDB
* FUSE (v.2.8.5)
* scons
* Boost (v1.49.0)

Authors
=======
Copyright 2014 Yaxing Chen

Building
========

::

 $ scons

Usage
=====

::

 $ ./mount_gridfs --db=db_name --host=localhost mount_point

Current Limitations
===================
* No Mongo authentication
