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

struct JournalHeader {  // 用于描述日志头
    uint32_t block_type;    // todo 块的类型，描述符块1或是提交块2,为什么这俩是并列的？
    uint32_t desc_num;          // 脏页的数量
    uint32_t desc_block_num;    // 要跟的描述符占的页数量
    uint32_t csum;  // todo 校验和?
};

struct DescritorInfo {  // 用于描述oid中的一个页面
    uint32_t page_len;   //page len in bytes，页面有多长，即在oid中划多长
    uint32_t off;   // 页面在oid中的偏移
    uint32_t reserve;   // reserve for future
    uint32_t oid_len;   // oid len in bytes
    ObjID oid;      // 页面在哪个oid中
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

// 由iod生成release key。
// inline函数为了效率：编译器将程序中出现的inline函数调用表达式用inline函数体进行替换。而对于其他的函数，都是在运行时候才被替代。
static inline ReleaseKey OidToReleaseKey(const ObjID& oid) {
    ReleaseKey release_key;
    memcpy(&release_key, &oid, sizeof(ObjID));
    return release_key;
}

// 由release key生成oid
static inline ObjID ReleaseKeyToOid(const ReleaseKey& release_key) {
    ObjID oid;
    memcpy(&oid, &release_key, sizeof(ReleaseKey));
    return oid;
}

// 日志的五个阶段：初始化、运行、等待提交、提交中、检查点
enum TransactionState {
    TX_INIT, TX_RUNNING, TX_WAITTING_COMMIT, TX_COMMITTING, TX_CHECKPOINT
};

class Transaction {
public:
    //TODO get dirty inodes
//    std::list<GCFSInode_Info*> dirty_inodes;
//  GCFSSuperBlock* fs_sb;
    pthread_mutex_t page_list_mutex;         // 用于保护dirty_pages
    std::list<std::shared_ptr<Page>> dirty_pages;   // 脏页列表, TODO may be sort for collect flush
    pthread_mutex_t release_obj_set_mutex;  // 用于保护release_obj_set
    std::set<ReleaseKey> release_obj_set;           // tx中释放的obj集合，用于在commit时，释放obj
    pthread_cond_t ref_cond;    // 用于在commit时，等待ref_count==0
    std::atomic<int> ref_count; // 用于在committing时，判断fs操作是否都已完成（ref_count==0）
    uint32_t block_num;         // 脏页以4K块为计量单位的数目. 列表中的page可能有一些是32KB的
    uint32_t used_block_num;    // 该tx总共使用的块数目（包括desc, pages, commit block）,用于checkpoint更新j_free
    uint32_t data_start_blknr;  // 该tx的数据区起始块号
    uint32_t tx_state;        // tx的状态

    Transaction();
    ~Transaction();
    void ReleaseObjSetInsert(const ObjID& objId);
    bool FindAndRemoveObj(const ObjID& objId);
};

#endif //GCFS_TRANSACTION_H
