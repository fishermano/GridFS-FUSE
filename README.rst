===========
GridFS FUSE
===========

Allows you to mount a MongoDB GridFS instance as a local filesystem.

Requirements
============

* A recent (v1.1.2 or later) MongoDB
* FUSE
* scons
* Boost

Authors
=======
Copyright 2009 Michael Stephens
Copyright 2014 陈亚兴（Modified/Updated）

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
