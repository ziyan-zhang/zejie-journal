//
// Created by huzj on 10/12/22.
//

#ifndef GCFS_JOURNAL_H
#define GCFS_JOURNAL_H


#include "transaction.h"

#define OBJECT_SIZE 4194304
#define JOURNAL_BLOCK_SIZE 4096
#define JOURNAL_BLOCK_SIZE_SHIFT 12
#define BLOCK_NUM_PER_OBJECT 1024 //4M obj has 1024 4K blocks
#define JOURNAL_OBJECT_MAX_NUM 256 //1G journal object
#define JOURNAL_BLOCK_MAX_NUM (JOURNAL_OBJECT_MAX_NUM*BLOCK_NUM_PER_OBJECT)


#define TX_MAX_BLOCK_NUM   4096 //4K*4096=16M the tx start to call commit
#define STOP_TX_MAX_BLOCK_NUM   (10*TX_MAX_BLOCK_NUM) //the tx stop to wait for commit done
#define JOURNAL_TX_MAX_NUM 200
#define JOURNAL_SPACE_EMERGENCY_RATIO 0.9
//#define JOURNAL_SUPERBLOCK_OBJECT "JSB"

constexpr int kCommitTimeIntervalMs = 5000;
constexpr int kCheckPointTimeIntervalMs = 5000;



struct JournalSuperBlock {
    //identifies the first unused block in the journal
    uint32_t j_head;
    //identifies the oldest used block in the journal
    uint32_t j_tail;
    //free block in the journal
    uint32_t j_free;
    //the start and the end block in the journal
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
    ObjStore* objstore;
    pthread_mutex_t journal_tx_mutex;
    pthread_cond_t commit_state_cond;
    Transaction* running_tx;
    Transaction* commit_tx;
    pthread_mutex_t cp_tx_list_mutex;  //TODO use spin lock?
    std::list<Transaction*> checkpoint_tx_list;

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
//        objstore->ObjRelease(objId);
    }

    void AllocObj(const ObjID& objId) {
        bool is_remove = false;
        if (this->running_tx != nullptr) {
            is_remove = this->running_tx->FindAndRemoveObj(objId);
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
    if (!page->Dirty() || page->NeedAddTxList()) {
        page->SetDirty();
        page->ClearAddTxList();
        pthread_mutex_lock(&tx->page_list_mutex);
        tx->dirty_pages.push_back(page);
        tx->block_num += (page->page_size / JOURNAL_BLOCK_SIZE);
        pthread_mutex_unlock(&tx->page_list_mutex);
    }
}


#endif //GCFS_JOURNAL_H
