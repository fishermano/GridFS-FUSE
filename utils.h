/*
 *  Copyright 2009 Michael Stephens
 *  Copyright 2014 陈亚兴
 
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

#ifndef __UTILS_H
#define __UTILS_H

#include <ctime>
#include <string>
#include <cstring>
#include <iostream>

#ifndef MAX_PATH_SIZE
#define MAX_PATH_SIZE 1024
#endif

inline const char* fuse_to_mongo_path(const char* path,bool is_file_name)
{

	if(!is_file_name){	
    	if(path[0] == '/') {
        	return path + 1;
    	} else {
        	return path;
   	 	}
	}else{
    	if(path[0] == '/') {
			char delim = '/';
			const char *name = strrchr(path,delim);
        	return name + 1;
    	} else {
        	return path;
    	}
	}
	
}

inline int get_depth(const char* path)
{
	int depth = 0;
	char path_t[MAX_PATH_SIZE];
	strcpy(path_t,path);
	char delims[] = "/";
	char *result = NULL;
	
	result = strtok(path_t,delims);
	while(result != NULL){
		depth++;
		result = strtok(NULL,delims);
	}

	return depth;
}

/*
inline void replace_substr(char* src_str, const char* search_str, const char* replace_str)
{
	
}
*/

inline std::string replace_substr(std::string src_str,std::string search_str,std::string replace_str)
{
	std::string::size_type pos = 0;
	if((pos=src_str.find(search_str, pos)) != std::string::npos){
		src_str.replace(pos, search_str.size(), replace_str);
	}
	
	return src_str;
}


inline time_t mongo_time_to_unix_time(unsigned long long mtime)
{
    return mtime / 1000;
}

inline time_t unix_time_to_mongo_time(unsigned long long utime)
{
    return utime * 1000;
}

inline time_t mongo_time()
{
    return unix_time_to_mongo_time(time(NULL));
}

inline std::string namespace_xattr(const std::string name)
{
#ifdef __linux__
    return "user." + name;
#else
    return name;
#endif
}

inline const char* unnamespace_xattr(const char* name) {
#ifdef __linux__
    if(std::strstr(name, "user.") == name) {
        return name + 5;
    }else if(std::strstr(name, "security.") == name){
	return name + 9;
    }else {
        return NULL;
    }
#else
    return name;
#endif
}

#endif
