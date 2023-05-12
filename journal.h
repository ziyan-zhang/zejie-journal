//
// Created by huzj on 10/12/22.
//

#ifndef GCFS_JOURNAL_H
#define GCFS_JOURNAL_H


#include "transaction.h"

#define OBJECT_SIZE 4194304
#define JOURNAL_BLOCK_SIZE 4096
#define JOURNAL_BLOCK_SIZE_SHIFT 12 // 2^12=4096
#define BLOCK_NUM_PER_OBJECT 1024 //4M obj has 1024 4K blocks
#define JOURNAL_OBJECT_MAX_NUM 256 //1G journal 有256个4M object
#define JOURNAL_BLOCK_MAX_NUM (JOURNAL_OBJECT_MAX_NUM*BLOCK_NUM_PER_OBJECT)


#define TX_MAX_BLOCK_NUM   4096 // tx 中的块累计到 4K*4096=16M 时达到最大值，开始调用commit（tx块还没那么多，异步commit）
// 下面这行的意思所有tx的总容量，如果超过10个最大的TX的容量，也就是160M，tx就要停止，等待commit完成
#define STOP_TX_MAX_BLOCK_NUM   (10*TX_MAX_BLOCK_NUM) // 停止tx，等待commit完成（tx块已足够多，同步commit）
#define JOURNAL_TX_MAX_NUM 200
#define JOURNAL_SPACE_EMERGENCY_RATIO 0.9
//#define JOURNAL_SUPERBLOCK_OBJECT "JSB"

constexpr int kCommitTimeIntervalMs = 5000; //5s
constexpr int kCheckPointTimeIntervalMs = 5000;



struct JournalSuperBlock {
    //identifies the first unused block in the journal
    uint32_t j_head;
    //identifies the oldest used block in the journal
    uint32_t j_tail;
    //free block in the journal
    uint32_t j_free;
    //the start and the end block in the journal
    // todo: j_head != j_last? , 另外，j_tail != j_first?
    uint32_t j_first;
    uint32_t j_last;
    //identifies the first unalloc object in the journal
    uint32_t j_alloc_head;
    uint32_t j_block_size;
};

class Journal {
private:
    pthread_mutex_t jsb_mutex;
    JournalSuperBlock* jsb;
    ObjID jsb_oid;
    GCFSSuperBlock_Info* sbi;
    ObjStore* objstore; // objstore应该是说这个journal存储在哪个对象存储对象上面。这里应该是Globalcache，不管是否持久吧，认为他能安全保存日志
    pthread_mutex_t journal_tx_mutex;
    pthread_cond_t commit_state_cond;   //TODO commit_state_cond是做什么的？
    Transaction* running_tx;    // 一个journal只有一个running tx，一个commit tx，一个checkpoint tx list(包含多个tx应该)
    Transaction* commit_tx;
    pthread_mutex_t cp_tx_list_mutex;  //TODO use spin lock?
    std::list<Transaction*> checkpoint_tx_list; // checkpoint tx list和保护list的互斥量

//    std::shared_ptr<Page> sb_page;
//    Page* sb_page;

    bool self_commit_terminate;
    bool self_checkpoint_terminate;
    bool journal_or_pc_capacity_emergent;
    bool read_only;
    pthread_mutex_t commit_mutex;
    pthread_mutex_t checkpoint_mutex;

    pthread_cond_t commit_start_cond;
    pthread_mutex_t commit_start_mutex;
    bool commit_start_flag;
    pthread_cond_t commit_finish_cond;
    pthread_mutex_t commit_finish_mutex;
    pthread_cond_t checkpoint_start_cond;
    pthread_mutex_t checkpoint_start_mutex;
    bool checkpoint_start_flag;
    pthread_cond_t checkpoint_finish_cond;
    pthread_mutex_t checkpoint_finish_mutex;

    pthread_t commit_thread;
    pthread_t checkpoint_thread;

    void JournalGetNextBlock(uint32_t& next_block_idx);
    void FlushJSB();

    GCFSErr CommitUnlock(bool force);
    void CheckpointUnlock();
    void JoinCommitThread();
    void JoinCheckpointThread();

public:
    Journal( GCFSSuperBlock_Info* sbi, ObjStore* objstore, bool read_only);
    ~Journal();
    Transaction* StartTx();
    void StopTx(Transaction* tx);
    void JournalGetWriteAccess(Transaction* tx, std::shared_ptr<Page> page);

    GCFSErr Commit(bool force = true);
    void PeriodCommit();
    void Checkpoint();
    void PeriodCheckpoint();
    void Recovery();
    GCFSErr StartCommitThread();
    void CallCommit();
    void WaitForCommitDone();

    GCFSErr StartCheckpointThread();
    void CallCheckpoint(bool capacity_emergent = false);
    void WaitForCheckpointDone();
    bool CheckPointListEmpty();

    int Sync();

    void ReleaseObj(Transaction* tx, const ObjID& objId) {
        tx->ReleaseObjSetInsert(objId);
//        objstore->ObjRelease(objId);  // 看来objstore中的对象释放没有在这里实现，compilot说在commit时实现了，后面注意一下
    }

    void AllocObj(const ObjID& objId) {
        bool is_remove = false;
        if (this->running_tx != nullptr) {
            is_remove = this->running_tx->FindAndRemoveObj(objId);  // todo: 什么意思, 这个remove怎么实现的？
        }
        if (this->commit_tx != nullptr) {
            is_remove = this->commit_tx->FindAndRemoveObj(objId);
        }
        objstore->ObjAlloc(objId);
    }
};

/**
 * page should be locked outside
 * @param tx
 * @param page
 */
static void MarkPageDirtyUnlock(Transaction* tx, std::shared_ptr<Page>& page) {
    if (!page->Dirty() || page->NeedAddTxList()) {  // 如果page不是dirty，或者page需要加入到tx的dirty page list中
        page->SetDirty();
        page->ClearAddTxList();
        pthread_mutex_lock(&tx->page_list_mutex);
        tx->dirty_pages.push_back(page);    // 将page加入到tx的dirty page list末尾
        tx->block_num += (page->page_size / JOURNAL_BLOCK_SIZE);
        pthread_mutex_unlock(&tx->page_list_mutex);
    }
}


#endif //GCFS_JOURNAL_H
