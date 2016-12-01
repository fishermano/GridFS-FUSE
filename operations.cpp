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

#include "operations.h"
#include "options.h"
#include "utils.h"
#include "local_gridfile.h"
#include <algorithm>
#include <cerrno>
#include <fcntl.h>

#include <mongo/s/chunk.h>
#include <mongo/client/dbclient.h>
#include <mongo/client/gridfs.h>
#include <mongo/client/connpool.h>

#include <boost/thread/recursive_mutex.hpp>
#include <boost/unordered_map.hpp>

#include <sys/types.h>
#include <unistd.h>


#ifdef __linux__
#include <attr/xattr.h>
#endif

#define DEBUG

#ifndef BLOCK_SIZE
#define BLOCK_SIZE (256*1024)
#endif

#ifndef MAX_PATH_SIZE
#define MAX_PATH_SIZE 1024
#endif

#ifndef WRONLY_MASK
#define WRONLY_MASK 128 //(--w-------)
#endif

#ifndef RDONLY_MASK
#define RDONLY_MASK 256 //(-r--------)
#endif

#ifndef EXEONLY_MASK
#define EXEONLY_MASK 64 //(---x------)
#endif


using namespace std;
using namespace mongo;

boost::unordered_map<string, LocalGridFile*> open_files;//储存已打开的文件列表

boost::unordered_map<string, mode_t> file_mode_s;//储存文件权限

unsigned int FH = 1;//储存文件句柄

boost::recursive_mutex flush_io_mutex;

boost::recursive_mutex map_io_mutex;

boost::recursive_mutex nlink_io_mutex;

/**
 * 获取文件属性
 * path：文件路径
 * stbuf：描述linux系统中文件属性的结构
 **/
int gridfs_getattr(const char *path, struct stat *stbuf)
{
	
	/*
	 * 初始化stbuf，0填充
	 */
    memset(stbuf, 0, sizeof(struct stat));
    
	/*
	 * 根目录“/”（注：挂载点即为gridfs-fuse的根目录）
	 */
    if(strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0775;//设置文件模式为目录且权限为777（rwxrwxrwx）
		int nlink_count = 0;
		try{
			/*
		 	 * 从连接池中获取一mongodb连接
		 	 */
    		ScopedDbConnection sdc(gridfs_options.host);
    		DBClientBase &conn = sdc.conn();
			#ifdef DEBUG
				printf("[GETATTR]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
			#endif
			string nodes_ns = string(gridfs_options.db)+string(".fs.nodes");//节点命名空间
			Query get_children_id(BSONObjBuilder().append("parent_id",OID(string("000000000000000000000000"))).obj());
			auto_ptr<DBClientCursor> get_children_id_c = conn.query(nodes_ns,get_children_id);
			while(get_children_id_c->more()){
				BSONObj child_obj = get_children_id_c->next();
				if(child_obj.getIntField("type")==1){
					nlink_count = nlink_count + 1;
				}
			}	
			sdc.done();
		}catch(DBException &e){
			cout<<"[GETATTR]: Error = "<<e.what()<<endl;
		}
        stbuf->st_nlink = nlink_count + 2;//设置目录的连接数
		stbuf->st_size = 1024;//设置目录的字节大小
        stbuf->st_ctime = time(NULL);//设置目录状态改变时间为当前时间
        stbuf->st_mtime = time(NULL);//设置目录最后被修改时间为当前时间
		stbuf->st_atime = time(NULL);//设置目录最近存取时间
        return 0;//<---成功返回
    }

	/*
	 * 在已打开文件中找到相应的文件
	 */
    boost::unordered_map<string,LocalGridFile*>::const_iterator file_iter;
    file_iter = open_files.find(path);
    if(file_iter != open_files.end()) {
        stbuf->st_mode = S_IFREG | file_mode_s[path];
        stbuf->st_nlink = 1;//设置文件的连接数为1
        stbuf->st_ctime = time(NULL);//设置文件状态改变时间为当前时间
        stbuf->st_mtime = time(NULL);//设置文件最后被修改时间为当前时间
		stbuf->st_atime = time(NULL);//设置文件最近存取时间
        stbuf->st_size = file_iter->second->getLength();//设置文件的字节大小
        return 0;//<--成功返回
    }

	try{
		/*
		 * 从连接池中获取一mongodb连接
		 */
    	ScopedDbConnection sdc(gridfs_options.host);
    	DBClientBase &conn = sdc.conn();
		#ifdef DEBUG
			printf("[GETATTR]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
		#endif
		string db_name = gridfs_options.db;//获取数据库名
		/*
    	 * 获取节点元信息
    	 */
		//boost::recursive_mutex::scoped_lock lock(getattr_io_mutex);
		BSONObj metedata_res = conn.findOne(db_name + ".fs.nodes",
                                      		BSON("abs_path" << path));
		if(!metedata_res.isEmpty()){
			int type = metedata_res.getIntField("type");
			BSONObj metedata_obj = metedata_res.getObjectField("meta_data");
			if(type==1){
				//目录
				stbuf->st_mode = S_IFDIR | metedata_obj.getIntField("mode");
				stbuf->st_nlink = metedata_obj.getIntField("nlink");
				if(metedata_obj.getIntField("uid") >= 0){
					stbuf->st_uid = metedata_obj.getIntField("uid");
				}
				if(metedata_obj.getIntField("gid") >= 0){
					stbuf->st_gid = metedata_obj.getIntField("gid");
				}
				stbuf->st_atime = metedata_obj.getField("atime").Date().toTimeT();
				stbuf->st_mtime = metedata_obj.getField("mtime").Date().toTimeT();
				stbuf->st_ctime = metedata_obj.getField("ctime").Date().toTimeT();
        		stbuf->st_size = 1024;//设置目录的字节大小
				stbuf->st_blksize = BLOCK_SIZE;
				stbuf->st_blocks = 1;
				sdc.done();
				return 0;//<---成功返回
			}else if(type==0){
				//文件
				//获取文件id
				OID file_id = metedata_obj.getField("file_id").OID();
				BSONObj file_obj = conn.findOne(db_name + ".fs.files",
                                      						BSON("_id" << file_id));
				if(!file_obj.isEmpty()){
        			stbuf->st_mode = S_IFREG | metedata_obj.getIntField("mode");//设置文件模式为一般文件且权限为666（rw-rw-rw-)
        			stbuf->st_nlink = metedata_obj.getIntField("nlink");//设置文件的连接数为1
					if(metedata_obj.getIntField("uid") >= 0){
						stbuf->st_uid = metedata_obj.getIntField("uid");
					}
					if(metedata_obj.getIntField("gid") >= 0){
						stbuf->st_gid = metedata_obj.getIntField("gid");
					}
					stbuf->st_atime = metedata_obj.getField("atime").Date().toTimeT();
        			stbuf->st_ctime = file_obj.getField("uploadDate").Date().toTimeT();
        			stbuf->st_mtime = metedata_obj.getField("mtime").Date().toTimeT();
        			stbuf->st_size = file_obj.getIntField("length");//设置文件的字节大小
					stbuf->st_blksize = BLOCK_SIZE;
					stbuf->st_blocks = file_obj.getIntField("chunkSize")/BLOCK_SIZE;
					sdc.done();
        			return 0;//<--成功返回
				}else{
					sdc.done();
					return -ENOENT;//<--没有相应的文件或文件夹
				}	
			}	
		}else{
			sdc.done();
			return -ENOENT;//<--没有相应的文件或文件夹
		}

		sdc.done();
	}catch(DBException &e){
		cout<<"[GETATTR]: Error = "<<e.what()<<endl;
	}
    return 0;//<--成功返回
}

/**
 * 读取目录内容
 * path：文件目录路径
 * buf：缓冲区
 * filler：函数指针，其作用为在readdir函数中增加一个目录项，每次往buf中填充一个目录项实体的信息。
 * offset：偏移量
 * fi：已打开文件信息
 **/
int gridfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                   off_t offset, struct fuse_file_info *fi)
{
	#ifdef DEBUG
	printf("[READDIR]: current path = \"%s\"\n",path);
	#endif
    filler(buf, ".", NULL, 0);//在当前目录下增加.目录
    filler(buf, "..", NULL, 0);//在当前目录下增加..目录

	try{
		/*
		 * 从连接池中获取一mongodb连接
		 */
    	ScopedDbConnection sdc(gridfs_options.host);
    	DBClientBase &conn = sdc.conn();
		#ifdef DEBUG
			printf("[READDIR]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
		#endif
		string nodes_ns = string(gridfs_options.db)+string(".fs.nodes");//节点命名空间
		string db_name = gridfs_options.db;//获取数据库名
		/*
	 	 * 判断目录类型
	 	 */
		if(strcmp(path,"/")==0){
			//根目录
			//遍历孩子节点id
			Query get_children_id(BSONObjBuilder().append("parent_id",OID(string("000000000000000000000000"))).obj());
			auto_ptr<DBClientCursor> get_children_id_c = conn.query(nodes_ns,get_children_id);
			while(get_children_id_c->more()){
				BSONObj child_obj = get_children_id_c->next();
				filler(buf,child_obj.getStringField("name"),NULL,0);//在当前目录下增加孩子节点name目录
			}	
		}else{
			//非根目录
			//获得当前目录节点id
			BSONObj parent_id_obj = conn.findOne(db_name + ".fs.nodes",
                                      						BSON("abs_path" << path));
			if(!parent_id_obj.isEmpty()){

				BSONObj metedata_obj = parent_id_obj.getObjectField("meta_data");
				if((metedata_obj.getIntField("mode") & RDONLY_MASK) != RDONLY_MASK){
					sdc.done();
					return -EACCES;
				}

				OID parent_id = parent_id_obj.getField("_id").OID();
				//遍历孩子节点id
				Query get_children_id(BSONObjBuilder().append("parent_id",parent_id).obj());
				auto_ptr<DBClientCursor> get_children_id_c = conn.query(nodes_ns,get_children_id);
				while(get_children_id_c->more()){
					BSONObj child_obj = get_children_id_c->next();
					filler(buf,child_obj.getStringField("name"),NULL,0);//在当前目录下增加孩子节点name目录
				}			
			}else{
				sdc.done();
				return -ENOENT;//<--没有相应的文件或文件夹
			}	
		}
		sdc.done();
	}catch(DBException &e){
		cout<<"[READDIR]: Error = "<<e.what()<<endl;
	}
    return 0;//<--成功返回
}

/**
 * 检查文件权限
 * path：文件路径
 * amode：检查项 06：检查读写权限 04：检查读权限 02：检查写权限 01：检查执行权限 00：检查文件的存在性
 **/
int gridfs_access(const char *path, int amode)
{
	if(strcmp(path,"/")==0){
		return 0;
	}

	if(amode == 0){
		#ifdef DEBUG
		printf("[ACCESS]: CHECKING \"%s\" EXIST\n",path);
		#endif
	}else if(amode = 1){
		#ifdef DEBUG
		printf("[ACCESS]: CHECKING \"%s\" EXE PERMISSION\n",path);
		#endif
		try{
			/*
		 	* 从连接池中获取一mongodb连接
		 	*/
        	ScopedDbConnection sdc(gridfs_options.host);
			#ifdef DEBUG
				printf("[ACCESS]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
			#endif
			DBClientBase &conn = sdc.conn();
			string db_name = gridfs_options.db;//获取数据库名
			/*
    		 * 获取节点元信息
    		 */
			BSONObj metedata_res = conn.findOne(db_name + ".fs.nodes",
                                      						BSON("abs_path" << path));
			if(!metedata_res.isEmpty()){
				BSONObj metedata_obj = metedata_res.getObjectField("meta_data");
				if((metedata_obj.getIntField("mode") & EXEONLY_MASK) != EXEONLY_MASK){
					sdc.done();
					return -EACCES;
				}
			}
			sdc.done();
		}catch(DBException &e){
			cout<<"[ACCESS]: Error = "<<e.what()<<endl;
		}
	}else if(amode = 2){
		#ifdef DEBUG
		printf("[ACCESS]: CHECKING \"%s\" WRITE PERMISSION\n",path);
		#endif
	}else if(amode = 4){
		#ifdef DEBUG
		printf("[ACCESS]: CHECKING \"%s\" READ PERMISSION\n",path);
		#endif
	}else if(amode = 6){
		#ifdef DEBUG
		printf("[ACCESS]: CHECKING \"%s\" READ WRITE PERMISSION\n",path);
		#endif
	}

	return 0;
}

/**
 * 打开文件
 * path：文件路径
 * fi：已打开文件信息
 **/
int gridfs_open(const char *path, struct fuse_file_info *fi)
{
	/*
	 * 判断文件访问模式
	 */
    if((fi->flags & O_ACCMODE) == O_RDONLY) {
		//文件只读
		#ifdef DEBUG
			printf("[OPEN]: FILE READ ONLY\n");
		#endif
		//在已打开文件中找到相应的文件
        if(open_files.find(path) != open_files.end()) {
            return 0;//<--成功返回
        }

		const char *name = fuse_to_mongo_path(path,false);//linux文件路径映射为mongodb文件路径

		try{
			/*
		 	* 从连接池中获取一mongodb连接
		 	*/
        	ScopedDbConnection sdc(gridfs_options.host);
			#ifdef DEBUG
				printf("[OPEN]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
			#endif
			DBClientBase &conn = sdc.conn();
			string db_name = gridfs_options.db;//获取数据库名
        	GridFS gf(conn, gridfs_options.db);
        	GridFile file = gf.findFile(name);//返回文件名为name的文件对象
		
			//检查GridFS的存在性
        	if(file.exists()) {
				/*
    			 * 获取节点元信息
    			 */
				BSONObj metedata_res = conn.findOne(db_name + ".fs.nodes",
                                      							BSON("abs_path" << path));
				if(!metedata_res.isEmpty()){
					BSONObj metedata_obj = metedata_res.getObjectField("meta_data");
					if((metedata_obj.getIntField("mode") & RDONLY_MASK) != RDONLY_MASK){
						sdc.done();
						return -EACCES;
					}
				}
				sdc.done();
            	return 0;//<--成功返回
        	}
		}catch(DBException &e){
			cout<<"[OPEN]: Error = "<<e.what()<<endl;
		}
	
        return -ENOENT;//<--没有相应的文件或文件夹
    } else if((fi->flags & O_ACCMODE) == O_WRONLY){
		//文件只写
		#ifdef DEBUG
			printf("[OPEN]: FILE WRITE ONLY\n");
		#endif
		fi->fh = FH;//设置文件句柄
		//在已打开文件中找到相应的文件
        if(open_files.find(path) != open_files.end()) {
            return 0;//<--成功返回
        }

		const char *name = fuse_to_mongo_path(path,false);//linux文件路径映射为mongodb文件路径

		try{
			/*
		 	* 从连接池中获取一mongodb连接
		 	*/
        	ScopedDbConnection sdc(gridfs_options.host);
			#ifdef DEBUG
				printf("[OPEN]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
			#endif
			DBClientBase &conn = sdc.conn();
			string db_name = gridfs_options.db;//获取数据库名
        	GridFS gf(conn, gridfs_options.db);
        	GridFile file = gf.findFile(name);//返回文件名为name的文件对象
		
			//检查GridFS的存在性
        	if(file.exists()) {	
				/*
    			 * 获取节点元信息
    			 */
				BSONObj metedata_res = conn.findOne(db_name + ".fs.nodes",
                                      							BSON("abs_path" << path));
				if(!metedata_res.isEmpty()){
					BSONObj metedata_obj = metedata_res.getObjectField("meta_data");
					if((metedata_obj.getIntField("mode") & WRONLY_MASK) != WRONLY_MASK){
						sdc.done();
						return -EACCES;
					}
					{
					boost::recursive_mutex::scoped_lock lock(map_io_mutex);
					file_mode_s.insert(boost::unordered_map<string, mode_t>::value_type(path,metedata_obj.getIntField("mode")));

					open_files.insert(boost::unordered_map<string, LocalGridFile*>::value_type(path,new LocalGridFile(DEFAULT_CHUNK_SIZE)));

					fi->fh = FH++;//设置文件句柄
					}
            	}
				sdc.done();
				return 0;//<--成功返回
        	}
			sdc.done();
		}catch(DBException &e){
			cout<<"[OPEN]: Error = "<<e.what()<<endl;
		}
	
		return -ENOENT;//<--没有相应的文件或文件夹
	}else if((fi->flags & O_ACCMODE) == O_RDWR){
		//文件读写
		#ifdef DEBUG
			printf("[OPEN]: FILE READ AND WRITE\n");
		#endif
		//在已打开文件中找到相应的文件
        if(open_files.find(path) != open_files.end()) {
            return 0;//<--成功返回
        }

		const char *name = fuse_to_mongo_path(path,false);//linux文件路径映射为mongodb文件路径

		try{
			/*
		 	* 从连接池中获取一mongodb连接
		 	*/
        	ScopedDbConnection sdc(gridfs_options.host);
			#ifdef DEBUG
				printf("[OPEN]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
			#endif
        	GridFS gf(sdc.conn(), gridfs_options.db);
        	GridFile file = gf.findFile(name);//返回文件名为name的文件对象
        	sdc.done();
		
			//检查GridFS的存在性
        	if(file.exists()) {
            	return 0;//<--成功返回
        	}
		}catch(DBException &e){
			cout<<"[OPEN]: Error = "<<e.what()<<endl;
		}

		return -ENOENT;//<--没有相应的文件或文件夹
	}else{
        return -EACCES;//<--权限错误，拒绝访问
    }
}

/**
 * 创建并打开文件
 * path：文件路径
 * mode：文件模式
 * ffi：已打开文件信息
 **/

//int gridfs_create(const char* path, mode_t mode, struct fuse_file_info* ffi)
//{
//	{
//	boost::recursive_mutex::scoped_lock lock(map_io_mutex);
//	if(strlen(path)>=MAX_PATH_SIZE){
//		return -ENAMETOOLONG;
//	}

//	try{
		/*
	 	 * 从连接池中获取一mongodb连接
	 	 */
//    	ScopedDbConnection sdc(gridfs_options.host);
//		DBClientBase &conn = sdc.conn();
//		string db_name = gridfs_options.db;//获取数据库名
//		#ifdef DEBUG
//			printf("[CREATE]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
//		#endif
//		string nodes_ns = string(gridfs_options.db)+string(".fs.nodes");//节点命名空间

//		char delim = '/';
//		const char *name = strrchr(path,delim);//获取目录名称

		/* 
		 * 获取父目录名称
		 */
//		string path_str(path);
//		int length = strlen(path)-strlen(name);
//		string parent_str = path_str.substr(0,length);
//		#ifdef DEBUG
//			printf("[CREATE]: PARENT = \"%s\"\n",parent_str.c_str());
//		#endif

//		if(parent_str == ""){
			//根目录
//			#ifdef DEBUG
//				printf("[CREATE]: PARENT ABS PATH = /\n");
//			#endif		
//		}else{
			//非根目录
//			#ifdef DEBUG
//				printf("[CREATE]: PARENT ABS PATH = \"%s\"\n",parent_str.c_str());
//			#endif
			
//			BSONObj parent_id_res = conn.findOne(db_name + ".fs.nodes",
//                                      				BSON("abs_path" << parent_str));
//			if(!parent_id_res.isEmpty()){

//				BSONObj meta_data_obj = parent_id_res.getObjectField("meta_data");
//				if((meta_data_obj.getIntField("mode") & (EXEONLY_MASK | WRONLY_MASK)) != (EXEONLY_MASK | WRONLY_MASK)){
//					sdc.done();
//					return -EACCES;
//				}
//			}
//		}
		
//		sdc.done();
//	}catch(DBException &e){
//		cout<<"[CREATE]: Error = "<<e.what()<<endl;
//	}

//	open_files.insert(boost::unordered_map<string, LocalGridFile*>::value_type(path,new LocalGridFile(DEFAULT_CHUNK_SIZE)));

//	file_mode_s.insert(boost::unordered_map<string, mode_t>::value_type(path,mode));

//    ffi->fh = FH++;//设置文件句柄
//	}

//    return 0;//<--成功返回
//}

int gridfs_mknod(const char* path, mode_t mode, dev_t dev)
{
	{
	boost::recursive_mutex::scoped_lock lock(map_io_mutex);
	if(strlen(path)>=MAX_PATH_SIZE){
		return -ENAMETOOLONG;
	}

	try{
		/*
	 	 * 从连接池中获取一mongodb连接
	 	 */
    	ScopedDbConnection sdc(gridfs_options.host);
		DBClientBase &conn = sdc.conn();
		string db_name = gridfs_options.db;//获取数据库名
		#ifdef DEBUG
			printf("[MKNOD]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
		#endif
		string nodes_ns = string(gridfs_options.db)+string(".fs.nodes");//节点命名空间

		char delim = '/';
		const char *name = strrchr(path,delim);//获取目录名称

		/* 
		 * 获取父目录名称
		 */
		string path_str(path);
		int length = strlen(path)-strlen(name);
		string parent_str = path_str.substr(0,length);
		#ifdef DEBUG
			printf("[MKNOD]: PARENT = \"%s\"\n",parent_str.c_str());
		#endif

		if(parent_str == ""){
			//根目录
			#ifdef DEBUG
				printf("[MKNOD]: PARENT ABS PATH = /\n");
			#endif		
		}else{
			//非根目录
			#ifdef DEBUG
				printf("[MKNOD]: PARENT ABS PATH = \"%s\"\n",parent_str.c_str());
			#endif
			
			BSONObj parent_id_res = conn.findOne(db_name + ".fs.nodes",
                                      				BSON("abs_path" << parent_str));
			if(!parent_id_res.isEmpty()){

				BSONObj meta_data_obj = parent_id_res.getObjectField("meta_data");
				if((meta_data_obj.getIntField("mode") & (EXEONLY_MASK | WRONLY_MASK)) != (EXEONLY_MASK | WRONLY_MASK)){
					sdc.done();
					return -EACCES;
				}
			}
		}
		
		sdc.done();
	}catch(DBException &e){
		cout<<"[MKNOD]: Error = "<<e.what()<<endl;
	}

	open_files.insert(boost::unordered_map<string, LocalGridFile*>::value_type(path,new LocalGridFile(DEFAULT_CHUNK_SIZE)));

	file_mode_s.insert(boost::unordered_map<string, mode_t>::value_type(path,mode));

	}

    return 0;//<--成功返回
}

/**
 * 释放已打开的文件
 * path：文件路径
 * ffi：已打开文件信息
 **/
int gridfs_release(const char* path, struct fuse_file_info* ffi)
{
	//句柄未设置，即为0
    if(!ffi->fh) {
        // fh is not set if file is opened read only
        // Would check ffi->flags for O_RDONLY instead but MacFuse doesn't
        // seem to properly pass flags into release
        return 0;//<--成功返回
    }

	{
	boost::recursive_mutex::scoped_lock lock(map_io_mutex);
    delete open_files[path];//释放open_files[path]指针指向的内存

	boost::unordered_map<string,LocalGridFile*>::iterator file_iter = open_files.find(path);
    open_files.erase(file_iter);//删除键为path的元素

	boost::unordered_map<string,mode_t>::iterator mode_iter = file_mode_s.find(path);
	file_mode_s.erase(mode_iter);
	}

    return 0;//<--成功返回
}

/**
 * 删除文件
 * path：文件路径
 **/
int gridfs_unlink(const char* path) {
    
	const char* file_name = fuse_to_mongo_path(path,false);//linux文件路径映射为mongodb文件路径

	try{
		/*
	 	* 从连接池中获取一mongodb连接
	 	*/
    	ScopedDbConnection sdc(gridfs_options.host);
		DBClientBase &conn = sdc.conn();
		string db_name = gridfs_options.db;//获取数据库名
		string nodes_ns = string(gridfs_options.db)+string(".fs.nodes");//节点命名空间
		#ifdef DEBUG
			printf("[UNLINK]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
		#endif

		char delim = '/';
		const char *name = strrchr(path,delim);//获取目录名称
			
		/* 
		 * 获取父目录名称
		 */
		string path_str(path);
		int length = strlen(path)-strlen(name);
		string parent_str = path_str.substr(0,length);
		#ifdef DEBUG
			printf("[UNLINK]: PARENT = \"%s\"\n",parent_str.c_str());
		#endif

		if(parent_str == ""){
			//根目录
			#ifdef DEBUG
				printf("[UNLINK]: PARENT ABS PATH = /\n");
			#endif		
		}else{
			//非根目录
			#ifdef DEBUG
				printf("[UNLINK]: PARENT ABS PATH = \"%s\"\n",parent_str.c_str());
			#endif

			BSONObj parent_id_res = conn.findOne(db_name + ".fs.nodes",
                                      				BSON("abs_path" << parent_str));
			if(!parent_id_res.isEmpty()){

				BSONObj meta_data_obj = parent_id_res.getObjectField("meta_data");
				if((meta_data_obj.getIntField("mode") & (EXEONLY_MASK | WRONLY_MASK)) != (EXEONLY_MASK | WRONLY_MASK)){
					sdc.done();
					return -EACCES;
				}

			}else{
				sdc.done();
    			return -ENOENT;//<--没有相应的文件或文件夹		
			}
		}

    	GridFS gf(conn, gridfs_options.db);
   	 	gf.removeFile(file_name);//删除文件名为name的文件

		/*
	 	 * 删除文件节点
	 	 */
		Query delete_file(BSONObjBuilder().append("abs_path",path).obj());
		conn.remove(nodes_ns,delete_file);
		#ifdef DEBUG
			printf("[UNLINK]: DELETE \"%s\" OK\n",path);
		#endif

		sdc.done();
	}catch(DBException &e){
		cout<<"[UNLINK]: Error = "<<e.what()<<endl;
	}
    return 0;//<--成功返回
}

/**
 * 读取数据（从已打开文件中）
 * path：文件路径
 * buf：缓存读出的数据
 * size：欲读出数据的大小
 * offset：读取数据的起始地址的数据偏移量
 * fi：已打开文件的信息
 **/
int gridfs_read(const char *path, char *buf, size_t size, off_t offset,
                struct fuse_file_info *fi)
{
	/*
	 * 根据文件路径查找相应的文件
	 */
    boost::unordered_map<string,LocalGridFile*>::const_iterator file_iter;
    file_iter = open_files.find(path);
    if(file_iter != open_files.end()) {
        LocalGridFile *lgf = file_iter->second;//实例化LocalGridFile
        return lgf->read(buf, size, offset);//读取偏移量为offset、大小为size的数据，并缓存于buf中
    }

	const char *name = fuse_to_mongo_path(path,false);//linux文件路径映射为mongodb文件路径

	size_t len = 0;//初始化已读取数据长度为0
	try{
		/*
	 	 * 从连接池中获取一mongodb连接
	 	 */
    	ScopedDbConnection sdc(gridfs_options.host);
		DBClientBase &conn = sdc.conn();
		#ifdef DEBUG
			printf("[READ]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
		#endif
    	GridFS gf(conn, gridfs_options.db);//获取一GridFS实例
    	GridFile file = gf.findFile(name);//返回文件名为path的文件对象

		//检查文件的存在性
   	 	if(!file.exists()) {
        	sdc.done();//关闭数据库连接
        	return -EBADF;//<--文件号错误
    	}

    	int chunk_size = file.getChunkSize();//获取文件块大小
    	int chunk_num = offset / chunk_size;//获取偏移数据量所占的块数（作为欲读取数据的首个块号）

		//循环读取块数据		
    	while(len < size && chunk_num < file.getNumChunks()) {
        	GridFSChunk chunk = file.getChunk(chunk_num);//获取块号为chunk_num的块数据
        	int to_read;
        	int cl = chunk.len();//块数据的大小

        	const char *d = chunk.data(cl);//获取长度为c1的块数据首地址

			//判断是否为首次读取数据
       	 	if(len) {
            	to_read = min((long unsigned)cl, (long unsigned)(size - len));
            	memcpy(buf + len, d, to_read);
        	} else {
            	to_read = min((long unsigned)(cl - (offset % chunk_size)), (long unsigned)(size - len));//计算首次读取数据的大小
            	memcpy(buf + len, d + (offset % chunk_size), to_read);//从d+(offset%chunk_size)所指地址处复制连续的to_read个字符到buf+len所指地址处。
        	}

        	len += to_read;//重新计算已读取数据长度
        	chunk_num++;//块号递增
    	}
    	sdc.done();
	}catch(DBException &e){
		cout<<"[READ]: Error = "<<e.what()<<endl;
	}

    return len;//<--返回已读取数据大小
}


/**
 * 将数据写入已打开文件
 * path：文件路径
 * buf：缓存写入的数据
 * nbyte：写入数据的大小
 * offset：写入数据起始偏移量
 * ffi：已打开文件的信息
 **/
int gridfs_write(const char* path, const char* buf, size_t nbyte,
                 off_t offset, struct fuse_file_info* ffi)
{
	/*
	 * 根据文件路径查找相应的文件
	 */
    if(open_files.find(path) == open_files.end()) {
        return -ENOENT;//<--没有相应的文件或文件夹
    }

    LocalGridFile *lgf_t = open_files[path];//获取LocalGridFile

    return lgf_t->write(buf, nbyte, offset);//写入数据
}

/**
 * 清除缓存数据
 * path：文件路径
 * ffi：已打开文件的信息
 **/
int gridfs_flush(const char* path, struct fuse_file_info *ffi)
{
	{
	boost::recursive_mutex::scoped_lock lock(flush_io_mutex);

    const char *name = fuse_to_mongo_path(path,false);//linux文件路径映射为mongodb文件路径
	const char *file_name = fuse_to_mongo_path(path,true);//linux文件路径映射为节点名

	//文件句柄为0
	if(!ffi->fh){
		return 0;
	}

	/*
	 * 根据文件路径查找相应的文件
	 */
    boost::unordered_map<string,LocalGridFile*>::iterator file_iter;
    file_iter = open_files.find(path);
    if(file_iter == open_files.end()) {
        return -ENOENT;//<--没有相应的文件或文件夹
    }

	/*
	 * 根据文件路径查找相应的文件权限
	 */
    boost::unordered_map<string,mode_t>::iterator mode_iter;
    mode_iter = file_mode_s.find(path);
    if(mode_iter == file_mode_s.end()) {
        return -EACCES;//<--相应文件权限错误
    }

    LocalGridFile *lgf = file_iter->second;//获取LocalGridFile对象

	if(lgf!=NULL){
	}else{
		return -EFAULT;
	}

	if(lgf->getLength()==0){
		return 0;
	}
	//文件已写入
    if(!lgf->dirty()) {
        return 0;//<--成功返回
    }

	try{
    	ScopedDbConnection sdc(gridfs_options.host);//从连接池中获取一mongodb连接
    	DBClientBase &conn = sdc.conn();//获取客户端
		string db_name = gridfs_options.db;//获取数据库名
		#ifdef DEBUG
			printf("[FLUSH]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
		#endif

		//检查节点存在性
		BSONObj node_obj = conn.findOne(db_name + ".fs.nodes",
                                      				BSON("abs_path" << path));
		if(!node_obj.isEmpty()){
			//节点存在
			#ifdef DEBUG
				printf("[FLUSH]: \"%s\" EXIST\n",path);
			#endif
    		GridFS gf(conn, gridfs_options.db);//获取一GridFS实例

    		size_t len = lgf->getLength();//获取文件长度
    		char *buf_t = new char[len];
    		lgf->read(buf_t, len, 0);//从已打开文件中读出数据

			gf.removeFile(file_name);
   	 		BSONObj file_obj = gf.storeFile(buf_t, len, name);//向数据库写入文件
			delete [] buf_t;
			OID file_id = file_obj.getField("_id").OID();

			/*
			 * 获取文档键集合
			 */
    		BSONObjBuilder b;
    		set<string> field_names;
    		node_obj.getFieldNames(field_names);//将文档中的所有键存入field_names中
			//过滤指定键
    		for(set<string>::iterator name = field_names.begin();
        		name != field_names.end(); name++)
    		{
				//键不是"filename"
      		  	if(*name != "meta_data") {
      		      	b.append(node_obj.getField(*name));
       		 	}
   	 		}

			BSONObj meta_data_obj = node_obj.getObjectField("meta_data");
    		BSONObjBuilder p;
    		set<string> p_field_names;
    		meta_data_obj.getFieldNames(p_field_names);//将文档中的所有键存入p_field_names中
			//过滤指定键
    		for(set<string>::iterator name = p_field_names.begin();
        		name != p_field_names.end(); name++)
    		{
				//键不是"filename"
      		  	if(*name != "file_id"){
      		      	p.append(meta_data_obj.getField(*name));
       		 	}
   	 		}

			p.append("file_id",file_id);

    		b << "meta_data" << p.obj();//添加filename键。

    		conn.update(db_name + ".fs.nodes",
                  		BSON("_id" << node_obj.getField("_id")), b.obj());//更新集合fs.files		
			sdc.done();
			lgf->flushed();//文件写入
			return 0;
		}else{
			//节点不存在，并创建
			#ifdef DEBUG
				printf("[FLUSH]: \"%s\" NOT EXIST\n",path);
			#endif
    		GridFS gf(conn, gridfs_options.db);//获取一GridFS实例

    		size_t len = lgf->getLength();//获取文件长度
    		char *buf_t = new char[len];
    		lgf->read(buf_t, len, 0);//从已打开文件中读出数据

   	 		BSONObj file_obj = gf.storeFile(buf_t, len, name);//向数据库写入文件
			delete [] buf_t;
			OID file_id = file_obj.getField("_id").OID();

			/*
			 * 计算父节点路径名
			 */
			string path_str(path);
			int length = strlen(path)-strlen(file_name);
			string parent_str = path_str.substr(0,length-1);
			#ifdef DEBUG
				printf("[FLUSH]: PARENT = \"%s\"\n",parent_str.c_str());
			#endif
			
			/*
			 * 计算父节点id
			 */
			OID parent_id;
			if(parent_str == ""){
				#ifdef DEBUG
					printf("[FLUSH]: PARENT ABS PATH = /\n");
				#endif
				parent_str = "000000000000000000000000";
				parent_id = OID(parent_str);
				
			}else{
				#ifdef DEBUG
					printf("[FLUSH]: PARENT ABS PATH = \"%s\"\n",parent_str.c_str());
				#endif
				BSONObj parent_id_res = conn.findOne(db_name + ".fs.nodes",
                                      					BSON("abs_path" << parent_str));
				if(!parent_id_res.isEmpty()){
					parent_id = parent_id_res.getField("_id").OID();
				}else{
					sdc.done();
					return -ENOENT;//<--没有相应的文件或文件夹	
				}
			}
			#ifdef DEBUG
				printf("[FLUSH]: PARENT ID = \"%s\"\n",parent_id.toString().c_str());
				printf("[FLUSH]: FILE NAME = \"%s\"\n",file_name);
			#endif
			//节点数据填充	
			string nodes_ns = string(gridfs_options.db)+string(".fs.nodes");//节点命名空间	
			BSONObj node = BSONObjBuilder().append("name",file_name).
											append("type",0).
											append("depth",get_depth(path)).
											append("abs_path",path).
											append("parent_id",parent_id).
											append("meta_data",BSONObjBuilder().
													append("file_id",file_id).
													append("mode",file_mode_s[path]).
													append("nlink",1).
													append("uid", getuid()).
													append("gid", getgid()).
													appendTimeT("atime",time(NULL)).
													appendTimeT("mtime",time(NULL)).
													appendTimeT("ctime",time(NULL)).obj()).
											obj();
			conn.insert(nodes_ns,node);
		}

		sdc.done();
	}catch(DBException &e){
		cout<<"[FLUSH]: Error = "<<e.what()<<endl;
	}  

    lgf->flushed();//文件写入
	}
    return 0;//<--成功返回
}

/**
 * 重命名
 * old_path：旧文件名
 * new_path：新文件名
 **/
int gridfs_rename(const char* old_path, const char* new_path)
{
    const char *old_name = fuse_to_mongo_path(old_path,false);//linux文件路径映射为mongodb文件路径
    const char *new_name = fuse_to_mongo_path(new_path,false);//linux文件路径映射为mongodb文件路径
	try{
		/*
	 	* 从连接池中获取一mongodb连接
	 	*/
    	ScopedDbConnection sdc(gridfs_options.host);
   	 	DBClientBase &conn = sdc.conn();
		#ifdef DEBUG
			printf("[RENAME]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
		#endif
    	string db_name = gridfs_options.db;//获取数据库名

		BSONObj new_node_obj = conn.findOne(db_name + ".fs.nodes",
									  BSON("abs_path" << new_path));
		if(!new_node_obj.isEmpty()){
			if(new_node_obj.getIntField("type")==0){
				gridfs_unlink(new_path);
			}
		}

    	BSONObj file_obj = conn.findOne(db_name + ".fs.files",
                                      BSON("filename" << old_name));//查找旧文件
   	 	BSONObj node_obj = conn.findOne(db_name + ".fs.nodes",
                                      BSON("abs_path" << old_path));//查找旧节点

		//检查文件的合法性
    	if(!file_obj.isEmpty()) {
			/*
			 * 获取文档键集合
			 */
    		BSONObjBuilder b;
    		set<string> field_names;
    		file_obj.getFieldNames(field_names);//将文档中的所有键存入field_names中
			//过滤指定键
    		for(set<string>::iterator name = field_names.begin();
        		name != field_names.end(); name++)
    		{
				//键不是"filename"
      		  	if(*name != "filename") {
      		      	b.append(file_obj.getField(*name));
       		 	}
   	 		}

    		b << "filename" << new_name;//添加filename键。

    		conn.update(db_name + ".fs.files",
                  		BSON("_id" << file_obj.getField("_id")), b.obj());//更新集合fs.files
		}

		//检查节点的合法性
   	 	if(node_obj.isEmpty()) {
				sdc.done();
    	    	return -ENOENT;//<--没有相应的文件或文件夹
   	 	}else{
			/*
			 * 获取文档键集合
			 */
			BSONObjBuilder r;
			set<string> node_field_names;
			node_obj.getFieldNames(node_field_names);

			//过滤指定键
   	 		for(set<string>::iterator name = node_field_names.begin();
        									name != node_field_names.end(); name++)
    		{
				//键不是"abs_path","name","parent_id","depth","meta_data"
        		if(*name != "abs_path") {
					if(*name != "name"){
						if(*name != "parent_id"){
							if(*name != "depth"){
								if(*name != "meta_data"){
            						r.append(node_obj.getField(*name));
								}
							}
						}
					}
        		}
    		}
		
			r << "abs_path" << new_path;//添加abs_path键
			const char* new_file_name = fuse_to_mongo_path(new_path,true);//linux文件路径映射为节点名
			r << "name" << new_file_name;//添加name键

			string nodes_ns = string(gridfs_options.db)+string(".fs.nodes");//节点命名空间
			/*
			 * 获取父节点路径名
			 */
			string path_str(new_path);
			int length = strlen(new_path)-strlen(new_file_name);
			string parent_str = path_str.substr(0,length-1);
			#ifdef DEBUG
				printf("[RENAME]: PARENT = \"%s\"\n",parent_str.c_str());
			#endif

			/*
			 * 获取父节点id
			 */
			OID parent_id;
			if(parent_str == ""){
				//根目录
				#ifdef DEBUG
					printf("[RENAME]: PARENT ABS PATH = /\n");
				#endif
				parent_str = "000000000000000000000000";

				parent_id = OID(parent_str);
				
			}else{
				//非根目录
				#ifdef DEBUG
					printf("[RENAME]: PARENT ABS PATH = \"%s\"\n",parent_str.c_str());
				#endif
				BSONObj parent_id_res = conn.findOne(db_name + ".fs.nodes",
                                      					BSON("abs_path" << parent_str));
				if(!parent_id_res.isEmpty()){
					parent_id = parent_id_res.getField("_id").OID();
				}else{
					sdc.done();
    				return -ENOENT;//<--没有相应的文件或文件夹	
				}
			}
			#ifdef DEBUG
				printf("[RENAME]: PARENT ID = \"%s\"\n",parent_id.toString().c_str());
			#endif

			r << "parent_id" << parent_id;//添加parent_id键
			r << "depth" << get_depth(new_path);//添加depth键

			BSONObj meta_data_obj = node_obj.getObjectField("meta_data");
    		BSONObjBuilder p;
    		set<string> p_field_names;
    		meta_data_obj.getFieldNames(p_field_names);//将文档中的所有键存入p_field_names中
			//过滤指定键
    		for(set<string>::iterator name = p_field_names.begin();
        		name != p_field_names.end(); name++)
    		{
				//键不是"filename"
      		  	if(*name != "mtime"){
					if(*name != "atime"){
      		      		p.append(meta_data_obj.getField(*name));
					}
       		 	}
   	 		}
			p.appendTimeT("atime",time(NULL));
			p.appendTimeT("mtime",time(NULL));

			r << "meta_data" << p.obj();

			conn.update(db_name + ".fs.nodes",
				 		BSON("_id" << node_obj.getField("_id")), r.obj());//更新集合fs.nodes

			//递归修改其子节点
			Query get_children(BSONObjBuilder().append("parent_id",node_obj.getField("_id").OID()).obj());
			auto_ptr<DBClientCursor> get_children_id_c = conn.query(nodes_ns,get_children);
			//迭代子节点id
			while(get_children_id_c->more()){
				BSONObj children_res = get_children_id_c->next();
				const char* old_path_t = children_res.getStringField("abs_path");
				const char* new_path_t = replace_substr(children_res.getStringField("abs_path"),string(old_path),string(new_path)).c_str();
			
				char old_path_e[MAX_PATH_SIZE] = {0};
				char new_path_e[MAX_PATH_SIZE] = {0};
				strcpy(new_path_e, new_path_t);
				strcpy(old_path_e, old_path_t);
				gridfs_rename((const char*)old_path_e, (const char*)new_path_e);		
			}
		}

		sdc.done();
	}catch(DBException &e){
		cout<<"[RENAME]: Error = "<<e.what()<<endl;
	}
    return 0;//<--成功返回
}

/**
 * 创建目录
 * path：目录名 * mode：访问模式
 **/
int gridfs_mkdir(const char* path,mode_t mode)
{

	if(strlen(path)>=MAX_PATH_SIZE){
		return -ENAMETOOLONG;
	}

	try{
		/*
		 * 从连接池中获取一mongodb连接
		 */
    	ScopedDbConnection sdc(gridfs_options.host);
		DBClientBase &conn = sdc.conn();
		string db_name = gridfs_options.db;//获取数据库名
		#ifdef DEBUG
			printf("[MKDIR]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
		#endif
		string nodes_ns = string(gridfs_options.db)+string(".fs.nodes");//节点命名空间

		/*
		 * 检查目录的存在性
		 */
		BSONObj dir_obj = conn.findOne(db_name + ".fs.nodes",
                                      				BSON("abs_path" << path));
		if(!dir_obj.isEmpty()){
			//目录存在
			#ifdef DEBUG
				printf("[MKDIR]: \"%s\" EXIST\n",path);
			#endif
			sdc.done();
			return -EEXIST;//文件或文件夹存在
		}else{
			//目录不存在，并创建
			#ifdef DEBUG
				printf("[MKDIR]: \"%s\" NOT EXIST\n",path);
			#endif

			char delim = '/';
			const char *name = strrchr(path,delim);//获取目录名称
			
			/* 
			 * 获取父目录名称
			 */
			string path_str(path);
			int length = strlen(path)-strlen(name);
			string parent_str = path_str.substr(0,length);
			#ifdef DEBUG
				printf("[MKDIR]: PARENT = \"%s\"\n",parent_str.c_str());
			#endif

			{
			boost::recursive_mutex::scoped_lock nlink_lock(nlink_io_mutex);
			/*
			 * 计算父目录id
			 */
			OID parent_id;
			if(parent_str == ""){
				//根目录
				#ifdef DEBUG
					printf("[MKDIR]: PARENT ABS PATH = /\n");
				#endif
				parent_str = "000000000000000000000000";

				parent_id = OID(parent_str);
				
			}else{
				//非根目录
				#ifdef DEBUG
					printf("[MKDIR]: PARENT ABS PATH = \"%s\"\n",parent_str.c_str());
				#endif

				BSONObj parent_id_res = conn.findOne(db_name + ".fs.nodes",
                                      					BSON("abs_path" << parent_str));
				if(!parent_id_res.isEmpty()){

					parent_id = parent_id_res.getField("_id").OID();

					BSONObjBuilder r;
					set<string> node_field_names;
					parent_id_res.getFieldNames(node_field_names);

					//过滤指定键
   	 				for(set<string>::iterator name = node_field_names.begin();
        											name != node_field_names.end(); name++)
    				{
						//键不是"meta_data"
        				if(*name != "meta_data") {
            				r.append(parent_id_res.getField(*name));
        				}
    				}

					BSONObj meta_data_obj = parent_id_res.getObjectField("meta_data");
					if((meta_data_obj.getIntField("mode") & (EXEONLY_MASK | WRONLY_MASK)) != (EXEONLY_MASK | WRONLY_MASK)){
						sdc.done();
						return -EACCES;
					}

    				BSONObjBuilder p;
    				set<string> p_field_names;
    				meta_data_obj.getFieldNames(p_field_names);//将文档中的所有键存入p_field_names中
					//过滤指定键
    				for(set<string>::iterator name = p_field_names.begin();
        											name != p_field_names.end(); name++)
    				{
						//键不是"filename"
      		  			if(*name != "nlink"){
      		      			p.append(meta_data_obj.getField(*name));	
       		 			}
   	 				}

					p << "nlink" << (meta_data_obj.getIntField("nlink") + 1);
					r << "meta_data" << p.obj();

					conn.update(db_name + ".fs.nodes",
				 		BSON("_id" << parent_id_res.getField("_id")), r.obj());//更新集合fs.nodes
				}else{
					sdc.done();
    				return -ENOENT;//<--没有相应的文件或文件夹		
				}
			}
			#ifdef DEBUG
				printf("[MKDIR]: PARENT ID = \"%s\"\n",parent_id.toString().c_str());
			#endif
				
			name = name + 1;//获取目录名
			#ifdef DEBUG
				printf("[MKDIR]: NAME = \"%s\"\n",name);
			#endif
			
			//目录节点数据填充
			BSONObj node = BSONObjBuilder().append("name",name).
											append("type",1).
											append("depth",get_depth(path)).
											append("abs_path",path).
											append("parent_id",parent_id).
											append("meta_data",BSONObjBuilder().
													append("file_id",OID(string("111111111111111111111111"))).
													append("mode",mode).
													append("nlink",2).
													append("uid", getuid()).
													append("gid", getgid()).
													appendTimeT("atime",time(NULL)).
													appendTimeT("mtime",time(NULL)).
													appendTimeT("ctime",time(NULL)).obj()).
											obj();
			conn.insert(nodes_ns,node);
			}
		}

		sdc.done();
	}catch(DBException &e){
		cout<<"[MKDIR]: Error = "<<e.what()<<endl;
	}

	return 0;
}

/**
 * 删除目录
 * path：目录名
 **/
int gridfs_rmdir(const char* path)
{
	try{
		/*
	 	 * 从连接池中获取一mongodb连接
	 	 */
    	ScopedDbConnection sdc(gridfs_options.host);
		DBClientBase &conn = sdc.conn();
		string db_name = gridfs_options.db;//获取数据库名
		#ifdef DEBUG
			printf("[RMDIR]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
		#endif
		string nodes_ns = string(gridfs_options.db)+string(".fs.nodes");//节点命名空间

		/*
		 * 检查目录的存在性
		 */
		BSONObj dir_obj = conn.findOne(db_name + ".fs.nodes",
                                      				BSON("abs_path" << path));

		if(!dir_obj.isEmpty()){
			char delim = '/';
			const char *name = strrchr(path,delim);//获取目录名称

			/* 
			 * 获取父目录名称
			 */
			string path_str(path);
			int length = strlen(path)-strlen(name);
			string parent_str = path_str.substr(0,length);
			#ifdef DEBUG
				printf("[RMDIR]: PARENT = \"%s\"\n",parent_str.c_str());
			#endif

			if(parent_str == ""){
				//根目录
				#ifdef DEBUG
					printf("[RMDIR]: PARENT ABS PATH = /\n");
				#endif		
			}else{
				//非根目录
				#ifdef DEBUG
					printf("[RMDIR]: PARENT ABS PATH = \"%s\"\n",parent_str.c_str());
				#endif
				{
				boost::recursive_mutex::scoped_lock nlink_lock(nlink_io_mutex);
				BSONObj parent_id_res = conn.findOne(db_name + ".fs.nodes",
                                      					BSON("abs_path" << parent_str));
				if(!parent_id_res.isEmpty()){

					BSONObjBuilder r;
					set<string> node_field_names;
					parent_id_res.getFieldNames(node_field_names);

					//过滤指定键
   	 				for(set<string>::iterator name = node_field_names.begin();
        											name != node_field_names.end(); name++)
    				{
						//键不是"meta_data"
        				if(*name != "meta_data") {
            				r.append(parent_id_res.getField(*name));
        				}
    				}

					BSONObj meta_data_obj = parent_id_res.getObjectField("meta_data");
					if((meta_data_obj.getIntField("mode") & (EXEONLY_MASK | WRONLY_MASK)) != (EXEONLY_MASK | WRONLY_MASK)){
						sdc.done();
						return -EACCES;
					}

    				BSONObjBuilder p;
    				set<string> p_field_names;
    				meta_data_obj.getFieldNames(p_field_names);//将文档中的所有键存入p_field_names中
					//过滤指定键
    				for(set<string>::iterator name = p_field_names.begin();
        											name != p_field_names.end(); name++)
    				{
						//键不是"filename"
      		  			if(*name != "nlink"){
      		      			p.append(meta_data_obj.getField(*name));	
       		 			}
   	 				}

					p << "nlink" << (meta_data_obj.getIntField("nlink") - 1);
					r << "meta_data" << p.obj();

					conn.update(db_name + ".fs.nodes",
				 		BSON("_id" << parent_id_res.getField("_id")), r.obj());//更新集合fs.nodes
				}else{
					sdc.done();
    				return -ENOENT;//<--没有相应的文件或文件夹		
				}
				}
			}

			/*
	 		* 删除目录节点
	 		*/
			Query delete_dir(BSONObjBuilder().append("abs_path",path).obj());
			conn.remove(nodes_ns,delete_dir);
			#ifdef DEBUG
				printf("[RMDIR]: DELETE \"%s\" OK\n",path);
			#endif

			sdc.done();
		}else{
			//目录不存在
			#ifdef DEBUG
				printf("[RMDIR]: \"%s\" NOT EXIST\n",path);
			#endif
			sdc.done();
			return -ENOENT;//文件或文件夹不存在	
		}

	}catch(DBException &e){
		cout<<"[RMDIR]: Error = "<<e.what()<<endl;
	}
	return 0;
}

/**
 * 将指定文件大小设置为length，若length小于原文件大小，超出部分被删掉
 * path：文件名
 * length：大小
 **/
int gridfs_truncate(const char* path,off_t length)
{
	const char* file_name = fuse_to_mongo_path(path,false);//linux文件路径映射为mongodb文件路径

	try{
		/*
	 	* 从连接池中获取一mongodb连接
	 	*/
    	ScopedDbConnection sdc(gridfs_options.host);
		DBClientBase &conn = sdc.conn();
		string db_name = gridfs_options.db;//获取数据库名
		string nodes_ns = string(gridfs_options.db)+string(".fs.nodes");//节点命名空间
		#ifdef DEBUG
			printf("[TRUNCATE]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
		#endif

		char delim = '/';
		const char *name = strrchr(path,delim);//获取目录名称
			
		/* 
		 * 获取父目录名称
		 */
		string path_str(path);
		int length = strlen(path)-strlen(name);
		string parent_str = path_str.substr(0,length);
		#ifdef DEBUG
			printf("[TRUNCATE]: PARENT = \"%s\"\n",parent_str.c_str());
		#endif

		if(parent_str == ""){
			//根目录
			#ifdef DEBUG
				printf("[TRUNCATE]: PARENT ABS PATH = /\n");
			#endif		
		}else{
			//非根目录
			#ifdef DEBUG
				printf("[TRUNCATE]: PARENT ABS PATH = \"%s\"\n",parent_str.c_str());
			#endif
			BSONObj parent_id_res = conn.findOne(db_name + ".fs.nodes",
                                      				BSON("abs_path" << parent_str));
			if(!parent_id_res.isEmpty()){

				BSONObj meta_data_obj = parent_id_res.getObjectField("meta_data");
				if((meta_data_obj.getIntField("mode") & (EXEONLY_MASK | WRONLY_MASK)) != (EXEONLY_MASK | WRONLY_MASK)){
					sdc.done();
					return -EACCES;
				}

			}else{
				sdc.done();
    			return -ENOENT;//<--没有相应的文件或文件夹		
			}
		}

    	GridFS gf(conn, gridfs_options.db);
   	 	gf.removeFile(file_name);//删除文件名为name的文件

		/*
	 	 * 删除文件节点
	 	 */
		Query delete_file(BSONObjBuilder().append("abs_path",path).obj());
		conn.remove(nodes_ns,delete_file);
		#ifdef DEBUG
			printf("[TRUNCATE]: DELETE \"%s\" OK\n",path);
		#endif

		sdc.done();
	}catch(DBException &e){
		cout<<"[TRUNCATE]: Error = "<<e.what()<<endl;
	}
    return 0;//<--成功返回
}

/**
 * 设置扩展属性
 * path：文件名
 * name：扩展属性名
 * value：扩展属性值
 * size：扩展属性大小
 * flags: 标志位
 **/
int gridfs_setxattr(const char* path, const char* name, const char* value, 
					size_t size, int flags)
{
	return 0;}

/**
 * 更改用户/组
 * path:文件名
 * uid:用户id
 * gid：组id
 **/
int gridfs_chown(const char* path, uid_t uid, gid_t gid)
{
	try{
		/*
	 	* 从连接池中获取一mongodb连接
	 	*/
    	ScopedDbConnection sdc(gridfs_options.host);
   	 	DBClientBase &conn = sdc.conn();
		#ifdef DEBUG
			printf("[CHOWN]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
		#endif
    	string db_name = gridfs_options.db;//获取数据库名

   	 	BSONObj node_obj = conn.findOne(db_name + ".fs.nodes",
                                      BSON("abs_path" << path));//查找节点

		//检查节点的合法性
   	 	if(node_obj.isEmpty()) {
				sdc.done();
    	    	return -ENOENT;//<--没有相应的文件或文件夹
   	 	}else{
			/*
			 * 获取文档键集合
			 */
			BSONObjBuilder r;
			set<string> node_field_names;
			node_obj.getFieldNames(node_field_names);

			//过滤指定键
   	 		for(set<string>::iterator name = node_field_names.begin();
        									name != node_field_names.end(); name++)
    		{
				//键不是"meta_data"
        		if(*name != "meta_data") {
            		r.append(node_obj.getField(*name));
        		}
    		}

			BSONObj meta_data_obj = node_obj.getObjectField("meta_data");
    		BSONObjBuilder p;
    		set<string> p_field_names;
    		meta_data_obj.getFieldNames(p_field_names);//将文档中的所有键存入p_field_names中
			//过滤指定键
    		for(set<string>::iterator name = p_field_names.begin();
        		name != p_field_names.end(); name++)
    		{
				//键不是"filename"
      		  	if(*name != "uid"){
					if(*name != "gid"){
      		      		p.append(meta_data_obj.getField(*name));
					}
       		 	}
   	 		}
			p << "uid" << uid;
			p << "gid" << gid;

			r << "meta_data" << p.obj();

			conn.update(db_name + ".fs.nodes",
				 		BSON("_id" << node_obj.getField("_id")), r.obj());//更新集合fs.nodes

		}
		
		sdc.done();
	}catch(DBException &e){
		cout<<"[CHOWN]: Error = "<<e.what()<<endl;
	}
	
	return 0;
}

/**
 * 更改用户/组的权限
 * path:文件名
 * mode:权限
 **/
int gridfs_chmod(const char* path,mode_t mode)
{	
	try{
		/*
	 	* 从连接池中获取一mongodb连接
	 	*/
    	ScopedDbConnection sdc(gridfs_options.host);
   	 	DBClientBase &conn = sdc.conn();
		#ifdef DEBUG
			printf("[CHMOD]: CONNECTED TO \"%s\" OK\n",gridfs_options.host);
		#endif
    	string db_name = gridfs_options.db;//获取数据库名

   	 	BSONObj node_obj = conn.findOne(db_name + ".fs.nodes",
                                      BSON("abs_path" << path));//查找节点

		//检查节点的合法性
   	 	if(node_obj.isEmpty()) {
				sdc.done();
    	    	return -ENOENT;//<--没有相应的文件或文件夹
   	 	}else{
			/*
			 * 获取文档键集合
			 */
			BSONObjBuilder r;
			set<string> node_field_names;
			node_obj.getFieldNames(node_field_names);

			//过滤指定键
   	 		for(set<string>::iterator name = node_field_names.begin();
        									name != node_field_names.end(); name++)
    		{
				//键不是"meta_data"
        		if(*name != "meta_data") {
            		r.append(node_obj.getField(*name));
        		}
    		}

			BSONObj meta_data_obj = node_obj.getObjectField("meta_data");
    		BSONObjBuilder p;
    		set<string> p_field_names;
    		meta_data_obj.getFieldNames(p_field_names);//将文档中的所有键存入p_field_names中
			//过滤指定键
    		for(set<string>::iterator name = p_field_names.begin();
        		name != p_field_names.end(); name++)
    		{
				//键不是"filename"
      		  	if(*name != "mode"){
      		      	p.append(meta_data_obj.getField(*name));
       		 	}
   	 		}
			p << "mode" << mode;

			r << "meta_data" << p.obj();

			conn.update(db_name + ".fs.nodes",
				 		BSON("_id" << node_obj.getField("_id")), r.obj());//更新集合fs.nodes

		}
		
		sdc.done();
	}catch(DBException &e){
		cout<<"[CHMOD]: Error = "<<e.what()<<endl;
	}
	
	return 0;
}

/**
 * 设定时间
 * path：文件名
 * ts:时间
 **/
int gridfs_utimens(const char *path, const struct timespec ts[2])
{
	return 0;
}
