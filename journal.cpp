//
// Created by huzj on 10/12/22.
//


#include <assert.h>
#include <cstring>
#include <sys/time.h>
#include "journal.h"

static void* commit_thread_func(void * args);
static void* checkpoint_thread_func(void * args);

void GetJoidAndOff(uint32_t block_nr, uint32_t& joid, uint32_t& off) {
    joid = block_nr / BLOCK_NUM_PER_OBJECT;
    off = ((block_nr & (BLOCK_NUM_PER_OBJECT-1)) << JOURNAL_BLOCK_SIZE_SHIFT);
}

Journal::Journal(GCFSSuperBlock_Info* sbi, ObjStore* objstore, bool read_only) {
    jsb = (JournalSuperBlock*) malloc(sizeof(JournalSuperBlock));
    jsb_oid = make_journal_root_oid();

    this->sbi = sbi;
    this->objstore = objstore;
    this->objstore->ObjRead(jsb_oid, (char*)jsb, sizeof(JournalSuperBlock), 0);
    this->read_only = read_only;

    LOG(INFO) << "journal jsb: " << "j_block_size: " << jsb->j_block_size << " jsb->j_first: " << jsb->j_first << "jsb->j_last: " << jsb->j_last
              << "jsb->j_free: " << jsb->j_free << "jsb->j_head: " << jsb->j_head << "jsb->j_tail: " << jsb->j_tail << "jsb->j_alloc_head: " << jsb->j_alloc_head;

    this->self_commit_terminate = false;
    this->self_checkpoint_terminate = false;
    this->journal_or_pc_capacity_emergent = false;
    this->commit_start_flag = false;
    this->checkpoint_start_flag = false;
    this->running_tx = nullptr;
    this->commit_tx = nullptr;


    pthread_mutex_init(&this->jsb_mutex, nullptr);
    pthread_mutex_init(&this->journal_tx_mutex, nullptr);
    pthread_cond_init(&this->commit_state_cond, nullptr);
    pthread_mutex_init(&this->cp_tx_list_mutex, nullptr);
    pthread_mutex_init(&this->commit_mutex, nullptr);
    pthread_mutex_init(&this->checkpoint_mutex, nullptr);

    pthread_cond_init(&this->commit_start_cond, nullptr);
    pthread_mutex_init(&this->commit_start_mutex, nullptr);
    pthread_cond_init(&this->commit_finish_cond, nullptr);
    pthread_mutex_init(&this->commit_finish_mutex, nullptr);
    pthread_cond_init(&this->checkpoint_start_cond, nullptr);
    pthread_mutex_init(&this->checkpoint_start_mutex, nullptr);
    pthread_cond_init(&this->checkpoint_finish_cond, nullptr);
    pthread_mutex_init(&this->checkpoint_finish_mutex, nullptr);
}

Journal::~Journal() {
    LOG(INFO) << "Journal start desturct";

    if (!read_only) {
        JoinCommitThread();
        LOG(INFO) << "JoinCommitThread done";
        JoinCheckpointThread();
        LOG(INFO) << "JoinCheckpointThread done";
        this->checkpoint_tx_list.clear();
    }

    SAFE_FREE(jsb);
    pthread_mutex_destroy(&this->jsb_mutex);
    pthread_mutex_destroy(&this->journal_tx_mutex);
    pthread_cond_destroy(&this->commit_state_cond);
    pthread_mutex_destroy(&this->cp_tx_list_mutex);
    pthread_mutex_destroy(&this->commit_mutex);
    pthread_mutex_destroy(&this->checkpoint_mutex);

    pthread_cond_destroy(&this->commit_start_cond);
    pthread_mutex_destroy(&this->commit_start_mutex);
    pthread_cond_destroy(&this->commit_finish_cond);
    pthread_mutex_destroy(&this->commit_finish_mutex);
    pthread_cond_destroy(&this->checkpoint_start_cond);
    pthread_mutex_destroy(&this->checkpoint_start_mutex);
    pthread_cond_destroy(&this->checkpoint_finish_cond);
    pthread_mutex_destroy(&this->checkpoint_finish_mutex);
}

Transaction* Journal::StartTx() {
//    LOG(INFO) << "StartTx";

    pthread_mutex_lock(&this->journal_tx_mutex);

    //waiting for teh last tx to be committed, new tx can't be started.
    if (this->commit_tx != nullptr && this->commit_tx->tx_state == TX_WAITTING_COMMIT) {
        pthread_cond_wait(&this->commit_state_cond, &this->journal_tx_mutex);
    }

    if (this->running_tx == nullptr) {
        this->running_tx = new Transaction();
        this->running_tx->tx_state = TX_RUNNING;
    }
    this->running_tx->ref_count++;
    Transaction* tx = this->running_tx;
//    LOG(INFO) << "StartTx: " << this->running_tx->ref_count;

    pthread_mutex_unlock(&this->journal_tx_mutex);
    return tx;
}

/**
 * page can't be lock when use StopTx, may give deadlock since it need to WaitForCommitDone
 * @param tx
 */
void Journal::StopTx(Transaction* tx) {
    pthread_mutex_lock(&this->journal_tx_mutex);
    tx->ref_count--;
    if (tx->ref_count <= 0) {
        //notify commit thread to start commit (if the commit thread is waiting)
        pthread_cond_signal(&tx->ref_cond);
    }

    if (tx->block_num >= STOP_TX_MAX_BLOCK_NUM && this->commit_tx != nullptr && this->commit_tx->tx_state == TX_COMMITTING) {
        //current tx is too large, block all the opreations, wait for the commit done
        WaitForCommitDone();
    }
//    LOG(INFO) << "StopTx: " << tx->ref_count;

    pthread_mutex_unlock(&this->journal_tx_mutex);

    if (tx->block_num >= TX_MAX_BLOCK_NUM) {
        //commit the tx async
        CallCommit();
    }

}

/**
 * page should be locked outside
 * @param page
 */
void Journal::JournalGetWriteAccess(Transaction* tx, std::shared_ptr<Page> page) {
    page->SetTxRunning();
    if (page->tx == nullptr) {
        page->tx = tx;
        //page is first appeared in this tx, should be add to page list
        page->SetAddTxList();
    } else if (page->tx != tx && !page->modified) {
        //in commit tx, copy the data to frozen data
        page->frozen_data = (char*)malloc(page->page_size);
        memcpy(page->frozen_data, page->data, page->page_size);
        page->modified = 1;
        page->next_tx = tx;
        page->SetAddTxList();
    }
}

void Journal::JournalGetNextBlock(uint32_t& next_block_idx) {
    pthread_mutex_lock(&this->jsb_mutex);
    next_block_idx = jsb->j_head;
    uint32_t j_oid_idx = next_block_idx / BLOCK_NUM_PER_OBJECT;
    if (jsb->j_alloc_head <= JOURNAL_OBJECT_MAX_NUM && jsb->j_alloc_head <= j_oid_idx) {
        //alloc the journal object
        ObjID j_oid = make_journal_block_oid(j_oid_idx);
        this->objstore->ObjAlloc(j_oid);
        jsb->j_alloc_head++;
    }
    jsb->j_head++;
    jsb->j_free--;
    if (jsb->j_head > jsb->j_last) jsb->j_head = jsb->j_first;
    pthread_mutex_unlock(&this->jsb_mutex);
}

void Journal::FlushJSB() {
    pthread_mutex_lock(&this->jsb_mutex);
    this->objstore->ObjWrite(jsb_oid, (char*)this->jsb, sizeof(JournalSuperBlock), 0);
    pthread_mutex_unlock(&this->jsb_mutex);
}

GCFSErr Journal::CommitUnlock(bool force) {
    LOG(INFO) << __func__ ;
    uint32_t desc_block_max_num, need_block_max_num;
    pthread_mutex_lock(&this->journal_tx_mutex);
    if (this->running_tx == nullptr) {
        pthread_mutex_unlock(&this->journal_tx_mutex);
        return GCFSErr_SUCCESS;
    }

    //commit tx must be null, switch the tx
    assert(this->commit_tx == nullptr);
    this->commit_tx = this->running_tx;
    this->running_tx = nullptr;
    if (commit_tx->ref_count > 0) {
        //if the waiting for commiting, new tx can't be started.
        LOG(INFO) << "Commit wait ..." ;
        this->commit_tx->tx_state = TX_WAITTING_COMMIT;
        pthread_cond_wait(&this->commit_tx->ref_cond, &this->journal_tx_mutex);
        this->commit_tx->tx_state = TX_COMMITTING;
        pthread_cond_broadcast(&this->commit_state_cond);
        LOG(INFO) << "Commit wait done" ;
    }
    this->commit_tx->tx_state = TX_COMMITTING;

    // add dirty super block page
    if (sbi->sbi_dirty) {
        sbi->sb_page->LockPage(PAGE_WRITE);
        memcpy(sbi->sb_page->data, (char*)&sbi->sb, sizeof(GCFSSuperBlock));
        memcpy(sbi->sb_page->data+(sbi->sb.s_root_inode_off), (char*)&sbi->root_inode.gcfs_inode, sizeof(GCFSInode));
        memcpy(sbi->sb_page->data+(sbi->sb.s_fiq_off), (char*)&sbi->fiq_groups, sizeof(FIQGroups));
        MarkPageDirtyUnlock(this->commit_tx, sbi->sb_page);
        sbi->sbi_dirty = false;
        sbi->sb_page->UnlockPage();
    }

    pthread_mutex_unlock(&this->journal_tx_mutex);

    if (this->commit_tx->dirty_pages.empty()) {
        delete commit_tx;
        this->commit_tx = nullptr;
        return GCFSErr_SUCCESS;
    }

    //desc block need to add header 64B
    desc_block_max_num = ((this->commit_tx->dirty_pages.size()+1) * DESCRITOR_INFO_MAX_SIZE + JOURNAL_BLOCK_SIZE - 1) / JOURNAL_BLOCK_SIZE;
    need_block_max_num = desc_block_max_num + this->commit_tx->block_num + 1; //add commit block

    while (need_block_max_num >= this->jsb->j_free) {
        if (!force) {
            //commit from checkpoint for some page which is modified by running tx, we can't checkpoint here (avoid dead lock)
            // return space is emergent
            return GCFSErr_JOURNAL_SPACE_EMERGENT;
        }
        //space is ememrgent, checkpoint to get journal space
        LOG(INFO) << "Commit CallCheckpoint";

        CallCheckpoint(true);
        WaitForCheckpointDone();

        LOG(INFO) << "Commit WaitForCheckpointDone";
    }
    //TODO add dirty fs superblock page, add dirty inode to dirty page,
    //iter commit the dirty page
    auto page_iter = this->commit_tx->dirty_pages.begin();
    auto flush_iter = page_iter;
    char* desc_block = (char*) malloc(JOURNAL_BLOCK_SIZE);
    uint32_t desc_block_off = 0;
    uint32_t block_nr, j_oid_idx, off, data_start_blknr, header_block_nr;
    bool is_header = true;
    JournalHeader* header = (JournalHeader*) &desc_block[desc_block_off];
    header->block_type = TX_DESCRIPTOR_BLOCK;
    header->desc_num = this->commit_tx->dirty_pages.size();
    header->desc_block_num = desc_block_max_num;
    desc_block_off = DESCRITOR_INFO_MAX_SIZE;

    //1. write descritor blocks
    while (true) {
        //TODO if page not dirty or release
        if (page_iter != this->commit_tx->dirty_pages.end() && !(*page_iter)->Dirty()) {
            this->commit_tx->block_num -= ((*page_iter)->page_size / JOURNAL_BLOCK_SIZE);
            this->commit_tx->dirty_pages.erase(page_iter++);
            continue;
        }

//        LOG(INFO) << "Commit write descritor blocks: " << block_nr;
        if (desc_block_off >= JOURNAL_BLOCK_SIZE || page_iter == this->commit_tx->dirty_pages.end()) {
            //descritor block full or the last page, flush descritor block

            JournalGetNextBlock(block_nr);
            if (is_header) {
                header_block_nr = block_nr;
                is_header = false;
            }
            GetJoidAndOff(block_nr, j_oid_idx, off);
            ObjID j_oid = make_journal_block_oid(j_oid_idx);
            this->objstore->ObjWrite(j_oid, desc_block, JOURNAL_BLOCK_SIZE, off);
            desc_block_off = 0;
            if (page_iter == this->commit_tx->dirty_pages.end()) break;
        }

//        uint32_t page_block_num = (*page_iter)->page_size / JOURNAL_BLOCK_SIZE;
        DescritorInfo* desc_info = (DescritorInfo*) &desc_block[desc_block_off];
        desc_info->page_len = (*page_iter)->page_size;
        desc_info->off = (*page_iter)->off;
        desc_info->oid_len = sizeof(ObjID);
        memcpy(&(desc_info->oid), &((*page_iter)->oid), desc_info->oid_len);
        desc_block_off += DESCRITOR_INFO_MAX_SIZE;

        page_iter ++;
    }

//    LOG(INFO) << "Commit write descritor blocks done";
    //recalculate since some not dirty pages were delete
    desc_block_max_num = ((this->commit_tx->dirty_pages.size()+1) * DESCRITOR_INFO_MAX_SIZE + JOURNAL_BLOCK_SIZE - 1) / JOURNAL_BLOCK_SIZE;
    need_block_max_num = desc_block_max_num + this->commit_tx->block_num + 1; //add commit block

    commit_tx->data_start_blknr = (block_nr + 1) % JOURNAL_BLOCK_MAX_NUM;
    commit_tx->used_block_num = need_block_max_num;

    header = (JournalHeader*) &desc_block[0];
    header->block_type = TX_DESCRIPTOR_BLOCK;
    header->desc_num = this->commit_tx->dirty_pages.size();
    header->desc_block_num = desc_block_max_num;
    GetJoidAndOff(header_block_nr, j_oid_idx, off);
    ObjID j_oid = make_journal_block_oid(j_oid_idx);
    this->objstore->ObjWrite(j_oid, desc_block, sizeof(JournalHeader), off);

    //2. flush dirty page block
    for (flush_iter = commit_tx->dirty_pages.begin(); flush_iter != commit_tx->dirty_pages.end(); flush_iter++) {
        if (flush_iter != this->commit_tx->dirty_pages.end() && !(*flush_iter)->Dirty()) {
            LOG(WARNING) << "Commit write blocks, page not dirty at all";
        }
        (*flush_iter)->LockPage(PAGE_WRITE);
        char* flush_data;
        if ((*flush_iter)->modified && (*flush_iter)->frozen_data != nullptr) {
            //page data was modified by the running tx, use the frozen_data
            flush_data = (*flush_iter)->frozen_data;
        } else {
            flush_data = (*flush_iter)->data;
        }
        for (int j = 0; j < ((*flush_iter)->page_size / JOURNAL_BLOCK_SIZE); ++j) {
            JournalGetNextBlock(block_nr);
            GetJoidAndOff(block_nr, j_oid_idx, off);
            j_oid = make_journal_block_oid(j_oid_idx);
//            LOG(INFO) << "start ObjWrite: " << "block_nr: " << block_nr << " j_oid_idx: " << j_oid_idx << " off: " << off << " data off: " << j*jsb->j_block_size << " data: " << flush_data[j*jsb->j_block_size];
            this->objstore->ObjWrite(j_oid, flush_data+(j*jsb->j_block_size), jsb->j_block_size, off);
        }
        if ((*flush_iter)->modified && (*flush_iter)->frozen_data != nullptr) {
            (*flush_iter)->modified = 0;
            free((*flush_iter)->frozen_data);
            (*flush_iter)->frozen_data = nullptr;
            (*flush_iter)->tx = this->running_tx;
            (*flush_iter)->next_tx = nullptr;
        } else {
            (*flush_iter)->ClearTxRunning();
            (*flush_iter)->tx = nullptr;
            (*flush_iter)->next_tx = nullptr;
        }
        (*flush_iter)->UnlockPage();
    }
//    LOG(INFO) << "Commit write data blocks done";


    timeval now;
    gettimeofday(&now,NULL);
    CommitHeader* commit_block = (CommitHeader*) malloc(sizeof(CommitHeader));
    commit_block->block_type = TX_COMMIT_BLOCK;
    commit_block->commit_sec = now.tv_sec;
    commit_block->commit_usec = now.tv_usec;
    JournalGetNextBlock(block_nr);
    GetJoidAndOff(block_nr, j_oid_idx, off);
    j_oid = make_journal_block_oid(j_oid_idx);
    this->objstore->ObjWrite(j_oid, (char*)commit_block, sizeof(CommitHeader), off);
    //flush journal superblock
    FlushJSB();

    //release obj in release set
    std::set<ReleaseKey>::iterator iter;
    pthread_mutex_lock(&this->commit_tx->release_obj_set_mutex);
    iter = this->commit_tx->release_obj_set.begin();
    for (iter = this->commit_tx->release_obj_set.begin(); iter != this->commit_tx->release_obj_set.end(); iter++) {
        this->objstore->ObjRelease(ReleaseKeyToOid(*iter));
    }
    this->commit_tx->release_obj_set.clear();
    pthread_mutex_unlock(&this->commit_tx->release_obj_set_mutex);


    this->commit_tx->tx_state = TX_CHECKPOINT;
    pthread_mutex_lock(&this->cp_tx_list_mutex);
    this->checkpoint_tx_list.push_back(this->commit_tx);
    pthread_mutex_unlock(&this->cp_tx_list_mutex);
    this->commit_tx = nullptr;

    SAFE_FREE(desc_block);
    SAFE_FREE(commit_block);



    LOG(INFO) << "Commit done";

    return GCFSErr_SUCCESS;
}

GCFSErr Journal::Commit(bool force) {
    GCFSErr res = GCFSErr_SUCCESS;
    pthread_mutex_lock(&this->commit_mutex);
    res = CommitUnlock(force);
    pthread_mutex_unlock(&this->commit_mutex);
    //notify finish
    pthread_mutex_lock(&this->commit_finish_mutex);
    pthread_cond_broadcast(&this->commit_finish_cond);
    pthread_mutex_unlock(&this->commit_finish_mutex);
    return res;
}


void Journal::CheckpointUnlock() {

    LOG(INFO) << __func__;
    GCFSErr ret = GCFSErr_SUCCESS;
    char* flush_data;
    char* temp_data = nullptr;
    uint32_t last_temp_page_size = 0;
    pthread_mutex_lock(&this->cp_tx_list_mutex);
    if (checkpoint_tx_list.empty()) {
        pthread_mutex_unlock(&this->cp_tx_list_mutex);
        return;
    }
    Transaction* cp_tx = checkpoint_tx_list.front();
    pthread_mutex_unlock(&this->cp_tx_list_mutex);

    uint32_t cur_journal_blknr = cp_tx->data_start_blknr;
    auto page_iter = cp_tx->dirty_pages.begin();
    while (page_iter != cp_tx->dirty_pages.end()) {
        std::shared_ptr<Page> page = *page_iter;
        page->LockPage(PAGE_WRITE);
        if (!page->Dirty()) {
            page->UnlockPage();
            page_iter++;
            continue;
        }

        //TODO don't use commit here
//        while (page->TxRunning() && ret == GCFSErr_SUCCESS) {
//            // page has been modify by running tx, if there is enough journal space, commit first
//            page->UnlockPage();
//            ret = Commit(false);
//            page->LockPage(PAGE_WRITE);
//        }

//        if (ret == GCFSErr_JOURNAL_SPACE_EMERGENT) {
        if (page->TxRunning()) {
            // we need to read the old version of the page from the journal, page is still dirty
            uint32_t j_oid_idx, off, read_size=0;
            if (last_temp_page_size < page->page_size) {
                if (temp_data != nullptr) free(temp_data);
                temp_data = (char*) malloc(page->page_size);
                last_temp_page_size = page->page_size;
            }
            GetJoidAndOff(cur_journal_blknr, j_oid_idx, off);
            while (read_size < page->page_size) {
                uint32_t cur_read_size = (OBJECT_SIZE-off) < (page->page_size-read_size) ? (OBJECT_SIZE-off) : (page->page_size-read_size);
                ObjID j_oid = make_journal_block_oid(j_oid_idx);
                this->objstore->ObjRead(j_oid, temp_data+read_size, cur_read_size, off);
                j_oid_idx = (j_oid_idx + 1) % JOURNAL_OBJECT_MAX_NUM;
                off = 0;
                read_size += cur_read_size;
            }
            flush_data = temp_data;
        } else {
            flush_data = page->data;
            page->ClearDirty();
        }
        this->objstore->ObjWrite(page->oid, flush_data, page->page_size, page->off);
        cur_journal_blknr = (cur_journal_blknr + (page->page_size / JOURNAL_BLOCK_SIZE)) % JOURNAL_BLOCK_MAX_NUM;
        page->UnlockPage();
        page_iter++;
    }
    //update jsb tail and free block num
    pthread_mutex_lock(&this->jsb_mutex);
    this->jsb->j_tail = (this->jsb->j_tail + cp_tx->used_block_num) % JOURNAL_BLOCK_MAX_NUM;
    this->jsb->j_free += cp_tx->used_block_num;
    this->objstore->ObjWrite(jsb_oid, (char*)this->jsb, sizeof(JournalSuperBlock), 0);
    pthread_mutex_unlock(&this->jsb_mutex);

    pthread_mutex_lock(&this->cp_tx_list_mutex);
    checkpoint_tx_list.pop_front();
    pthread_mutex_unlock(&this->cp_tx_list_mutex);

    if (temp_data != nullptr) free(temp_data);
    delete cp_tx;
    LOG(INFO) << "Checkpoint done";
}

void Journal::Checkpoint() {
    pthread_mutex_lock(&this->checkpoint_mutex);
    CheckpointUnlock();
    pthread_mutex_unlock(&this->checkpoint_mutex);
    //notify finish
    this->journal_or_pc_capacity_emergent = false;
    pthread_mutex_lock(&this->checkpoint_finish_mutex);
    pthread_cond_broadcast(&this->checkpoint_finish_cond);
    pthread_mutex_unlock(&this->checkpoint_finish_mutex);
}

void Journal::Recovery() {
    //TODO 1. scan the journal, tail to head; 2. flush journal page; 3. update jsb
    LOG(INFO) << __func__ ;
    uint32_t j_oid_idx, off, read_size, total_size, last_write_buf_size, last_desc_buf_size=0;
    bool need_update = false;
    char* write_buf = nullptr;
    char* desc_buf = nullptr;
    JournalHeader journal_header;
    DescritorInfo* desc_info;
    LOG(INFO) << "jsb->j_tail: " << jsb->j_tail << " jsb->j_head: " << jsb->j_head;
    while (jsb->j_tail != jsb->j_head) {
        need_update = true;
        GetJoidAndOff(jsb->j_tail, j_oid_idx, off);
        ObjID j_oid = make_journal_block_oid(j_oid_idx);
        objstore->ObjRead(j_oid, (char*)&journal_header, sizeof(JournalHeader), off);
        total_size = journal_header.desc_num*DESCRITOR_INFO_MAX_SIZE;

        if (last_desc_buf_size < total_size) {
            if (desc_buf != nullptr) free(desc_buf);
            desc_buf = (char*) malloc(total_size);
            last_desc_buf_size = total_size;
        }
//        char* desc_buf = (char*)malloc(total_size);
        read_size = 0;
        off += DESCRITOR_INFO_MAX_SIZE;
        while (read_size < total_size) {
            uint32_t cur_read_size = (OBJECT_SIZE-off) < (total_size-read_size) ? (OBJECT_SIZE-off) : (total_size-read_size);
            j_oid = make_journal_block_oid(j_oid_idx);
            objstore->ObjRead(j_oid, desc_buf+read_size, cur_read_size, off);
            j_oid_idx = (j_oid_idx + 1) % JOURNAL_OBJECT_MAX_NUM;
            off = 0;
            read_size += cur_read_size;
        }
        jsb->j_tail = (jsb->j_tail + journal_header.desc_block_num) % JOURNAL_BLOCK_MAX_NUM;
        jsb->j_free += journal_header.desc_block_num;

        last_write_buf_size = 0;
        for (int i = 0; i < journal_header.desc_num; ++i) {
            desc_info = (DescritorInfo*) (desc_buf+i*DESCRITOR_INFO_MAX_SIZE);
            if (last_write_buf_size < desc_info->page_len) {
                if (write_buf != nullptr) free(write_buf);
                write_buf = (char*) malloc(desc_info->page_len);
                last_write_buf_size = desc_info->page_len;
            }
            GetJoidAndOff(jsb->j_tail, j_oid_idx, off);
            read_size = 0;
            while (read_size < desc_info->page_len) {
                uint32_t cur_read_size = (OBJECT_SIZE-off) < (desc_info->page_len-read_size) ? (OBJECT_SIZE-off) : (desc_info->page_len-read_size);
                j_oid = make_journal_block_oid(j_oid_idx);
                this->objstore->ObjRead(j_oid, write_buf+read_size, cur_read_size, off);
                j_oid_idx = (j_oid_idx + 1) % JOURNAL_OBJECT_MAX_NUM;
                off = 0;
                read_size += cur_read_size;
            }

            objstore->ObjWrite(desc_info->oid, write_buf, desc_info->page_len, desc_info->off);
            jsb->j_tail = (jsb->j_tail + (desc_info->page_len / JOURNAL_BLOCK_SIZE)) % JOURNAL_BLOCK_MAX_NUM;
            jsb->j_free += (desc_info->page_len / JOURNAL_BLOCK_SIZE);
        }
        jsb->j_tail = (jsb->j_tail+1) % JOURNAL_BLOCK_MAX_NUM; //commit block
        jsb->j_free++;
        //finish one tx, flush journal super block
        FlushJSB();
    }
    //all the journal recovery done, journal is empty now
    if (need_update) {
        jsb->j_tail = 0;
        jsb->j_head = 0;
        jsb->j_free = JOURNAL_BLOCK_MAX_NUM;
        FlushJSB();
        if (desc_buf != nullptr) free(desc_buf);
        if (write_buf != nullptr) free(write_buf);
    }

}

static void* commit_thread_func(void * args) {
    Journal* journal = (Journal*) args;
    journal->PeriodCommit();
}

GCFSErr Journal::StartCommitThread() {
    int err;
    err = pthread_create(&this->commit_thread, nullptr, commit_thread_func, this);
    if (err != 0)
        return GCFSErr_JOURNAL_THREAD_START_ERR;
    return GCFSErr_SUCCESS;
}

void Journal::JoinCommitThread() {
    pthread_mutex_lock(&this->commit_start_mutex);
    this->self_commit_terminate = true;
    pthread_cond_signal(&this->commit_start_cond);
    pthread_mutex_unlock(&this->commit_start_mutex);
    pthread_join(this->commit_thread, nullptr);
}

void get_cond_wait_abstime(struct timespec& out_abstime, const int time_interval) {
    struct timeval now;
    gettimeofday(&now, nullptr);
    long nsec = now.tv_usec * 1000 + (time_interval % 1000) * 1000000;
    out_abstime.tv_sec = now.tv_sec + nsec / 1000000000 + time_interval / 1000;
    out_abstime.tv_nsec = nsec % 1000000000;
}


void Journal::PeriodCommit() {
    timespec out_abstime;
    GCFSErr res;
    while (!this->self_commit_terminate) {
        get_cond_wait_abstime(out_abstime, kCommitTimeIntervalMs);
        pthread_mutex_lock(&this->commit_start_mutex);
        if (!this->commit_start_flag)
            pthread_cond_timedwait(&this->commit_start_cond, &this->commit_start_mutex, &out_abstime);
        else
            this->commit_start_flag = false;
        pthread_mutex_unlock(&this->commit_start_mutex);
        Commit();
    }
}

void Journal::CallCommit() {
    pthread_mutex_lock(&this->commit_start_mutex);
    this->commit_start_flag = true;
    pthread_cond_signal(&this->commit_start_cond);
    pthread_mutex_unlock(&this->commit_start_mutex);
}

void Journal::WaitForCommitDone() {
    pthread_mutex_lock(&this->commit_finish_mutex);
    pthread_cond_wait(&this->commit_finish_cond, &this->commit_finish_mutex);
    pthread_mutex_unlock(&this->commit_finish_mutex);
}

static void* checkpoint_thread_func(void * args) {
    Journal* journal = (Journal*) args;
    journal->PeriodCheckpoint();
}

GCFSErr Journal::StartCheckpointThread() {
    int err;
    err = pthread_create(&this->checkpoint_thread, nullptr, checkpoint_thread_func, this);
    if (err != 0)
        return GCFSErr_JOURNAL_THREAD_START_ERR;
    return GCFSErr_SUCCESS;
}

void Journal::PeriodCheckpoint() {
    timespec out_abstime;
    GCFSErr res;
    while (!this->self_checkpoint_terminate) {
        get_cond_wait_abstime(out_abstime, kCheckPointTimeIntervalMs);
        pthread_mutex_lock(&this->checkpoint_start_mutex);
        if (!this->checkpoint_start_flag)
            pthread_cond_timedwait(&this->checkpoint_start_cond, &this->checkpoint_start_mutex, &out_abstime);
        else
            this->checkpoint_start_flag = false;
        pthread_mutex_unlock(&this->checkpoint_start_mutex);
        Checkpoint();
        if (this->self_checkpoint_terminate) {
//            //TODO checkpoint all
            res = Commit(false);
            while (!this->checkpoint_tx_list.empty()) {
                Checkpoint();
            }
            if (res == GCFSErr_JOURNAL_SPACE_EMERGENT) {
                res = Commit(false);
                while (!this->checkpoint_tx_list.empty()) {
                    Checkpoint();
                }
            }

        }
    }
}

void Journal::CallCheckpoint(bool capacity_emergent) {
    pthread_mutex_lock(&this->checkpoint_start_mutex);
    if (capacity_emergent) this->journal_or_pc_capacity_emergent = true;
    this->checkpoint_start_flag = true;
    pthread_cond_signal(&this->checkpoint_start_cond);
    pthread_mutex_unlock(&this->checkpoint_start_mutex);
}

void Journal::WaitForCheckpointDone() {
    pthread_mutex_lock(&this->checkpoint_finish_mutex);
    pthread_cond_wait(&this->checkpoint_finish_cond, &this->checkpoint_finish_mutex);
    pthread_mutex_unlock(&this->checkpoint_finish_mutex);
}

void Journal::JoinCheckpointThread() {
    pthread_mutex_lock(&this->checkpoint_start_mutex);
    this->self_checkpoint_terminate = true;
    pthread_cond_signal(&this->checkpoint_start_cond);
    pthread_mutex_unlock(&this->checkpoint_start_mutex);
    pthread_join(this->checkpoint_thread, nullptr);
}

bool Journal::CheckPointListEmpty() {
    return this->checkpoint_tx_list.empty();
}

int Journal::Sync() {
    CallCommit();
    WaitForCommitDone();
    return 0;
}
