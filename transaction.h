//
// Created by huzj on 10/12/22.
//

#ifndef GCFS_TRANSACTION_H
#define GCFS_TRANSACTION_H


#include <list>
#include <unordered_set>
#include <set>
#include <string>
#include <fs.h>
#include "page.h"
#include "../objstore/objstore.h"

#define TX_DESCRIPTOR_BLOCK 1
#define TX_COMMIT_BLOCK 2

struct JournalHeader {
    uint32_t block_type;
    uint32_t desc_num;
    uint32_t desc_block_num;
    uint32_t csum;
};

struct DescritorInfo {
    uint32_t page_len;               //page len in bytes
    uint32_t off;
    uint32_t reserve;
    uint32_t oid_len;
    ObjID oid;
};

//#define DESCRITOR_INFO_MAX_SIZE 64  //TODO should change to sizeof(DescritorInfo)
#define DESCRITOR_INFO_MAX_SIZE sizeof(DescritorInfo)

struct CommitHeader {
    uint32_t block_type;
    uint32_t csum;
    uint64_t commit_sec;
    uint64_t commit_usec;
};

struct ReleaseKey {
    uint64_t release_key_h;
    uint64_t release_key_l;
    bool operator == (const ReleaseKey& key) const {
        if (release_key_h == key.release_key_h && release_key_l ==key.release_key_l) return true;
        else return false;
    }

    bool operator < (const ReleaseKey& key) const {
        if (release_key_h != key.release_key_h) return (release_key_h < key.release_key_h);
        else return (release_key_l < key.release_key_l);
    }
};

static inline ReleaseKey OidToReleaseKey(const ObjID& oid) {
    ReleaseKey release_key;
    memcpy(&release_key, &oid, sizeof(ObjID));
    return release_key;
}

static inline ObjID ReleaseKeyToOid(const ReleaseKey& release_key) {
    ObjID oid;
    memcpy(&oid, &release_key, sizeof(ReleaseKey));
    return oid;
}

enum TransactionState {
    TX_INIT, TX_RUNNING, TX_WAITTING_COMMIT, TX_COMMITTING, TX_CHECKPOINT
};

class Transaction {
public:
    //TODO get dirty inodes
//    std::list<GCFSInode_Info*> dirty_inodes;
//  GCFSSuperBlock* fs_sb;
    pthread_mutex_t page_list_mutex;
    std::list<std::shared_ptr<Page>> dirty_pages;    //TODO may be sort for collect flush
    pthread_mutex_t release_obj_set_mutex;
    std::set<ReleaseKey> release_obj_set;
    pthread_cond_t ref_cond;
    std::atomic<int> ref_count;                 //used to judge if the fs ops are all done (ref_count == 0), for committing
    uint32_t block_num;                         //dirty page 4K block num. since the page in list may be some page with 32KB
    uint32_t used_block_num;                    //total used block num of the tx(include desc, pages, commit block), for checkpoint to update the j_free
    uint32_t data_start_blknr;
    uint32_t tx_state;

    Transaction();
    ~Transaction();
    void ReleaseObjSetInsert(const ObjID& objId);
    bool FindAndRemoveObj(const ObjID& objId);
};

#endif //GCFS_TRANSACTION_H
