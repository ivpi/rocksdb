//  Copyright (c) 2019, Samsung Electronics.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  Written by Ivan L. Picoli <i.picoli@samsung.com>

#include "env/env_zns.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include <util/coding.h>
#include <iostream>
#include <atomic>
#include <string.h>
#include <sys/time.h>

using namespace std;

namespace rocksdb {


/* ### SequentialFile method implementation ### */

Status ZNSSequentialFile::Read(size_t n, Slice* result, char* scratch) {
    cout << "WARNING: " << __func__ << " name:  " << filename_
						<< " size: " << n << endl;

    *result = Slice(scratch, n);

    return Status::OK();
}

Status ZNSSequentialFile::PositionedRead(uint64_t offset, size_t n,
					    Slice* result, char* scratch) {
    cout << "WARNING: " << __func__ << " offset: " << offset
						<< " n: " << n << endl;

    *result = Slice(scratch, n);

    return Status::OK();
}

Status ZNSSequentialFile::Skip(uint64_t n) {
    cout << "WARNING: " << __func__ << " n: " << n << endl;
    return Status::OK();
}

Status ZNSSequentialFile::InvalidateCache(size_t offset, size_t length) {
    if (ZNS_DEBUG) cout << __func__ << " offset: " << offset << " length: "
							<< length << endl;
    return Status::OK();
}


/* ### RandomAccessFile method implementation ### */

Status ZNSRandomAccessFile::ReadObj(uint64_t offset, size_t n, Slice* result,
							char* scratch) const {
    int ret;

    if (ZNS_DEBUG_R)
	cout << __func__ << "name: " << filename_ << " offset: "
					    << offset << " size: " << n << endl;

    ret = zrocks_read_obj (ztl_id, offset, scratch, n);
    if (ret){
	cout << " ZRocks (read_obj) error: " << ret << endl;
	return Status::IOError();
    }

    *result = Slice(scratch, n);

    return Status::OK();
}

Status ZNSRandomAccessFile::ReadOffset(uint64_t offset, size_t n, Slice* result,
							char* scratch) const {
    int ret;
    size_t piece_off = 0, off, left, size;
    unsigned i;
    struct zrocks_map *map;

    if (ZNS_DEBUG_R)
	cout << __func__ << " name: " << filename_ << " offset: "
					    << offset << " size: " << n << endl;

    /* Find the first piece of mapping */
    off = 0;
    for (i = 0; i < env_zns->files[filename_]->map.size(); i++) {
	map = &env_zns->files[filename_]->map.at(i);

	size = map->g.nsec * ZNS_ALIGMENT;
	if (off + size > offset) {
	    piece_off = size - ( off + size - offset);
	    break;
	}

	off += size;
    }

    if (i == env_zns->files[filename_]->map.size())
	return Status::IOError();

    /* Create one read per piece */
    left = n;
    while (left) {
	map = &env_zns->files[filename_]->map.at(i);

	size = map->g.nsec * ZNS_ALIGMENT;
	size = (size - piece_off > left) ? left : size - piece_off;
	off  = map->g.offset * ZNS_ALIGMENT;

	if (ZNS_DEBUG_R) cout << __func__ << "  map " << i << " piece(0x" <<
	    std::hex << map->g.offset << "/" << std::dec << map->g.nsec <<
	    ") read(" << off << ":" << piece_off << "/" << size << ")" <<
	    " left " << left << endl;

	off += piece_off;
	ret = zrocks_read (off, scratch + (n - left), size);
	if (ret) {
	    cout << " ZRocks (read) error: " << ret << endl;

	    for (i = 0; i < env_zns->files[filename_]->map.size(); i++) {
		map = &env_zns->files[filename_]->map.at(i);
		cout << __func__ << "  [map " << std::hex << i << std::dec <<
		" off " << map[i].g.offset << " sz " << map[i].g.nsec << "]" << endl;
	    }
	    return Status::IOError();
	}

	off += size;
	left -= size;
	piece_off = 0;
	i++;
    }

    *result = Slice(scratch, n);

    return Status::OK();
}

Status ZNSRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result,
							char* scratch) const {

#if ZNS_PREFETCH
    atomic_flag* flag = const_cast<atomic_flag*>(&prefetch_lock);

    while (flag->test_and_set(std::memory_order_acquire));

    if ( (prefetch_sz > 0) && (offset >= prefetch_off) &&
			      (offset + n <= prefetch_off + prefetch_sz) ) {

	memcpy(scratch, prefetch + (offset - prefetch_off), n);
	flag->clear(std::memory_order_release);
	*result = Slice(scratch, n);

	return Status::OK();
    }

    flag->clear(std::memory_order_release);
#endif

#if ZNS_OBJ_STORE
    return ReadObj (offset, n, result, scratch);
#else
    return ReadOffset (offset, n, result, scratch);
#endif
}

Status ZNSRandomAccessFile::Prefetch(uint64_t offset, size_t n) {
    if (ZNS_DEBUG) cout << __func__ << " offset: " << offset
					    << " n: " << n << endl;

#if ZNS_PREFETCH
    Slice result;
    Status st;
    atomic_flag* flag = const_cast<atomic_flag*>(&prefetch_lock);

    while (flag->test_and_set(memory_order_acquire));

#if ZNS_OBJ_STORE
    st = ReadObj (offset, n, &result, prefetch);
#else
    st = ReadOffset (offset, n, &result, prefetch);
#endif
    if (!st.ok()) {
	prefetch_sz = 0;
	flag->clear(memory_order_release);
	return Status::OK();
    }

    prefetch_sz = n;
    prefetch_off = offset;

    flag->clear(memory_order_release);
#endif /* ZNS_PREFETCH */

    return Status::OK();
}

size_t ZNSRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
    if (ZNS_DEBUG) cout << __func__ << endl;

    if (max_size < (kMaxVarint64Length * 3)) {
	return 0;
    }

    char* rid = id;
    rid = EncodeVarint64(rid, (uint64_t)this);
    rid = EncodeVarint64(rid, (uint64_t)this);
    rid = EncodeVarint64(rid, (uint64_t)this);
    assert(rid >= id);

    return static_cast<size_t>(rid - id);
}

Status ZNSRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
    if (ZNS_DEBUG) cout << __func__ << " offset: " << offset
					    << " length: " << length << endl;
    return Status::OK();
}


/* ### WritableFile method implementation ### */

Status ZNSWritableFile::Append(const Slice& data) {
    if (ZNS_DEBUG_AF) cout << __func__ << " size: " << data.size() << endl;

    if (!cache_off)
	return Status::IOError();

    if (cache_off + data.size() > wcache + ZNS_MAX_BUF) {
	cout << __func__ << " Maximum buffer size is 64 MB" << endl;
	return Status::IOError();
    }

    memcpy (cache_off, data.data(), data.size());
    cache_off += data.size();
    filesize_ += data.size();

    env_zns->files[filename_]->size += data.size();

    return Status::OK();
}

Status ZNSWritableFile::PositionedAppend(const Slice& data, uint64_t offset) {
    if (ZNS_DEBUG_AF) cout << __func__ << __func__ << " size: " << data.size()
					    << " offset: " << offset << endl;
    if (offset != filesize_) {
	cout << "Write Violation: " << __func__ << " size: " << data.size()
					    << " offset: " << offset << endl;
    }

    return Append(data);
}

Status ZNSWritableFile::Truncate(uint64_t size) {
    if (ZNS_DEBUG_AF) cout << __func__ << " size: " << size << endl;
    return Status::OK();
}

Status ZNSWritableFile::Close() {
    if (ZNS_DEBUG) cout << __func__ << endl;
    return Status::OK();
}

Status ZNSWritableFile::Flush() {
    if (ZNS_DEBUG_AF) cout << __func__ << endl;
    return Status::OK();
}

Status ZNSWritableFile::Sync() {
    size_t size;
    int ret;

#if !ZNS_OBJ_STORE
    struct zrocks_map *map;
    uint16_t pieces = 0;
    int i;
#endif

    if (!cache_off)
	return Status::OK();

    size = (size_t) (cache_off - wcache);
    if (ZNS_DEBUG_W) cout << __func__ << " flush size: " << size << endl;

    /* Create space for persistent mapping entries */
    memset (cache_off, 0, sizeof (struct zrocks_map) * ZNS_MAX_MAP_ENTS);
    size += sizeof (struct zrocks_map) * ZNS_MAX_MAP_ENTS;

#if ZNS_OBJ_STORE
    ret = zrocks_new (ztl_id, wcache, size, level);
#else
    ret = zrocks_write (wcache, size, level, &map, &pieces);
#endif

    map_off = map[pieces - 1].g.offset;
    memcpy (cache_off, map, sizeof (struct zrocks_map) * pieces);

    // Make sure the mapping piece is accessible via MANIFEST

    if (ret) {
	cout << " ZRocks (write) error: " << ret << endl;
	return Status::IOError();
    }

#if !ZNS_OBJ_STORE
    if (ZNS_DEBUG_W) cout << __func__ << " DONE. pieces: " << pieces << endl;

    for (i = 0; i < pieces; i++) {
	env_zns->files[filename_]->map.push_back(map[i]);

	if (ZNS_DEBUG_W) cout << __func__ << "  map 0x" << std::hex << i << std::dec <<
		" off " << map[i].g.offset << " sz " << map[i].g.nsec << endl;
    }
    zrocks_free (map);
#endif

    cache_off = wcache;

    return Status::OK();
}

Status ZNSWritableFile::Fsync() {
    if (ZNS_DEBUG_AF) cout << __func__ << endl;
    return Sync();
}

Status ZNSWritableFile::InvalidateCache(size_t offset, size_t length) {
    if (ZNS_DEBUG) cout << __func__ << " offset: " << offset
				    << " length: " << length << endl;
    return Status::OK();
}

#ifdef ROCKSDB_FALLOCATE_PRESENT
Status ZNSWritableFile::Allocate(uint64_t offset, uint64_t len) {
    if (ZNS_DEBUG) cout << __func__ << " offset: " << offset
					<< " len: " << len << endl;
    return Status::OK();
}
#endif

Status ZNSWritableFile::RangeSync(uint64_t offset, uint64_t nbytes) {
    cout << "WARNING: " << __func__ << " offset: " << offset
					<< " nbytes: " << nbytes << endl;
    return Status::OK();
}

size_t ZNSWritableFile::GetUniqueId(char* id, size_t max_size) const {
    if (ZNS_DEBUG) cout << __func__ << endl;

    if (max_size < (kMaxVarint64Length * 3)) {
	return 0;
    }

    char* rid = id;
    rid = EncodeVarint64(rid, (uint64_t)this);
    rid = EncodeVarint64(rid, (uint64_t)this);
    rid = EncodeVarint64(rid, (uint64_t)this);
    assert(rid >= id);

    return static_cast<size_t>(rid - id);
}

} // namespace rocksdb
