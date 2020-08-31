//  Copyright (c) 2019, Samsung Electronics.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  Written by Ivan L. Picoli <i.picoli@samsung.com>

#include "env/env_zns.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include <iostream>
#include <sys/time.h>

using namespace std;

namespace rocksdb {

/* ### ZNS Environment method implementation ### */

Status ZNSEnv::NewSequentialFile(const std::string& fname,
	    		   std::unique_ptr<SequentialFile>* result,
			   const EnvOptions& options) {
    if (ZNS_DEBUG) cout << __func__ <<  ":" << fname << endl;

    if (IsFilePosix(fname)) {
	return posixEnv->NewSequentialFile(fname, result, options);
    }

    result->reset();

    ZNSSequentialFile *f = new ZNSSequentialFile (fname, options);
    result->reset(dynamic_cast<SequentialFile*>(f));

    return Status::OK();
}

Status ZNSEnv::NewRandomAccessFile(const std::string& fname,
			    std::unique_ptr<RandomAccessFile>* result,
			    const EnvOptions& options) {
    if (ZNS_DEBUG) cout << __func__ <<  ":" << fname << endl;

    if (IsFilePosix(fname)) {
	return posixEnv->NewRandomAccessFile(fname, result, options);
    }

    ZNSRandomAccessFile *f = new ZNSRandomAccessFile (fname, this, options);
    result->reset(dynamic_cast<RandomAccessFile*>(f));

    return Status::OK();
}

Status ZNSEnv::NewWritableFile(const std::string& fname,
			    std::unique_ptr<WritableFile>* result,
			    const EnvOptions& options) {
    if (ZNS_DEBUG) cout << __func__ <<  ":" << fname << endl;

    if (IsFilePosix(fname)) {
	return posixEnv->NewWritableFile(fname, result, options);
    }

    ZNSWritableFile *f = new ZNSWritableFile (fname, this, options, -1);
    result->reset(dynamic_cast<WritableFile*>(f));

    if (files.count(fname) == 0)
	files[fname] = new ZNSFile(fname, -1);

    return Status::OK();
}

Status ZNSEnv::NewWritableLeveledFile(const std::string& fname,
			    std::unique_ptr<WritableFile>* result,
			    const EnvOptions& options,
			    int level) {
    if (ZNS_DEBUG) cout << __func__ <<  ":" << fname << " lvl: "
							<< level << endl;

    if (IsFilePosix(fname)) {
	return posixEnv->NewWritableFile(fname, result, options);
    }

    ZNSWritableFile *f = new ZNSWritableFile (fname, this, options, level);
    result->reset(dynamic_cast<WritableFile*>(f));

    if (files.count(fname) == 0)
	files[fname] = new ZNSFile(fname, level);

    return Status::OK();
}

Status ZNSEnv::DeleteFile(const std::string& fname) {
#if !ZNS_OBJ_STORE
    unsigned i;
    struct zrocks_map *map;
#endif

    if (ZNS_DEBUG) cout << __func__ << ":" << fname << endl;

    if (IsFilePosix(fname)) {
	return posixEnv->DeleteFile(fname);
    }

#if !ZNS_OBJ_STORE
    if (files.find(fname) == files.end()) {
	return Status::OK();
    }
    for (i = 0; i < files[fname]->map.size(); i++) {
	map = &files[fname]->map.at(i);
	zrocks_trim (map, files[fname]->level);
    }
#endif

    delete files[fname];
    files.erase(fname);

    return Status::OK();
}

Status ZNSEnv::GetFileSize(const std::string& fname, uint64_t* size) {
    if (IsFilePosix(fname)) {
	if (ZNS_DEBUG) cout << __func__ << ":" << fname << endl;
	return posixEnv->GetFileSize(fname, size);
    }

    if (files.find(fname) == files.end()) {
	return Status::OK();
    }

    if (ZNS_DEBUG)
	cout << __func__ << ":" << fname << "size: " << files[fname]->size << endl;

    *size = files[fname]->size;

    return Status::OK();
}

Status ZNSEnv::GetFileModificationTime(const std::string& fname,
				 uint64_t* file_mtime) {
    if (ZNS_DEBUG) cout << __func__ << ":" << fname << endl;

    if (IsFilePosix(fname)) {
	return posixEnv->GetFileModificationTime(fname, file_mtime);
    }

    /* TODO: Get SST files modification time from ZNS */
    *file_mtime = 0;

    return Status::OK();
}


/* ### The factory method for creating a ZNS Env ### */

Status NewZNSEnv(Env** zns_env, const std::string& dev_name) {
    *zns_env = new ZNSEnv(dev_name);
    return Status::OK();
}

} // namespace rocksdb
