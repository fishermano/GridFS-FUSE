/*
 *  Copyright 2009 Michael Stephens
 *  Copyright 2014 陈亚兴（Modified/Updated）
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _LOCAL_GRIDFILE_H
#define _LOCAL_GRIDFILE_H

#include <vector>
#include <cstring>
#include <iostream>

const unsigned int DEFAULT_CHUNK_SIZE = 256 * 1024;

class LocalGridFile {
public:
    LocalGridFile(int chunkSize = DEFAULT_CHUNK_SIZE) :
    _chunkSize(chunkSize), _length(0), _dirty(true) {
          _chunks.push_back(new char[_chunkSize]);
      }

    ~LocalGridFile() {
		
        for(std::vector<char*>::iterator i = _chunks.begin();
            i != _chunks.end(); i++) {
            delete *i;
        }
    }

    int getChunkSize() { return _chunkSize; }
    int getNumChunks() { return _chunks.size(); }
    int getLength() { return _length; }
    char* getChunk(int n) { return _chunks[n]; }
    bool dirty() { return _dirty; }
    void flushed() { _dirty = false; }

    int write(const char* buf, size_t nbyte, off_t offset);
    int read(char* buf, size_t size, off_t offset);

private:
    int _chunkSize, _length;
    bool _dirty;
    std::vector<char*> _chunks;
};

#endif
