//
// Created by huzj on 10/12/22.
//


#include <assert.h>
#include <cstring>
#include <sys/time.h>
#include "journal.h"

static void* commit_thread_func(void * args);       // 根据args得到journal对象，然后调用journal的PeriodCommit
static void* checkpoint_thread_func(void * args);   // 根据args得到journal对象，然后调用journal的PeriodCheckpoint

// 从块编号求出块落在哪个oid里面以及块在oid中的偏移量
void GetJoidAndOff(uint32_t block_nr, uint32_t& joid, uint32_t& off) {
    joid = block_nr / BLOCK_NUM_PER_OBJECT; // 求出块编号落在哪个对象里面。4MB大小的对象含有BLOCK_NUM_PER_OBJECT=1024个4KB的块
    off = ((block_nr & (BLOCK_NUM_PER_OBJECT-1)) << JOURNAL_BLOCK_SIZE_SHIFT);  // 求出块编号落在对象中的偏移量
    // off = (block_nr % BLOCK_NUM_PER_OBJECT) * JOURNAL_BLOCK_SIZE; // 上面用了位与操作和移位操作，等效于这一行
}

Journal::Journal(GCFSSuperBlock_Info* sbi, ObjStore* objstore, bool read_only) {
    jsb = (JournalSuperBlock*) malloc(sizeof(JournalSuperBlock));
    jsb_oid = make_journal_root_oid();

    this->sbi = sbi;    // GCFSSuperBlock_Info* sbi;
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
    this->running_tx = nullptr;     // journal初始化的时候，没有正在运行的事务和正在提交的事务
    this->commit_tx = nullptr;

    // 超级块锁, journal_tx（running tx?）锁, commit state条件变量， checkpoint tx list锁, commit锁, checkpoint锁
    pthread_mutex_init(&this->jsb_mutex, nullptr);
    pthread_mutex_init(&this->journal_tx_mutex, nullptr);
    pthread_cond_init(&this->commit_state_cond, nullptr);
    pthread_mutex_init(&this->cp_tx_list_mutex, nullptr);
    pthread_mutex_init(&this->commit_mutex, nullptr);
    pthread_mutex_init(&this->checkpoint_mutex, nullptr);

    // commit start条件变量， commit finish条件变量， checkpoint start条件变量， checkpoint finish条件变量
    // commit start锁， commit finish锁， checkpoint start锁， checkpoint finish锁
    pthread_cond_init(&this->commit_start_cond, nullptr);
    pthread_mutex_init(&this->commit_start_mutex, nullptr);
    pthread_cond_init(&this->commit_finish_cond, nullptr);
    pthread_mutex_init(&this->commit_finish_mutex, nullptr);
    pthread_cond_init(&this->checkpoint_start_cond, nullptr);
    pthread_mutex_init(&this->checkpoint_start_mutex, nullptr);
    pthread_cond_init(&this->checkpoint_finish_cond, nullptr);
    pthread_mutex_init(&this->checkpoint_finish_mutex, nullptr);
}

Journal::~Journal() {   // todo: 这里析构的时候，objid和objstore, running_tx, commit_tx都没有释放?
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

    // 等待上一个事务提交完成，新的事务才能开始。
    // commit 等待（事务中所有的文件系统操作完成）阶段，这个过程中running tx会被阻塞，没有新的页更新操作。
    if (this->commit_tx != nullptr && this->commit_tx->tx_state == TX_WAITTING_COMMIT) {
        pthread_cond_wait(&this->commit_state_cond, &this->journal_tx_mutex);
    }

    // todo: 这里一定是空吗？commit_tx由WAITTING_COMMIT变为TX_COMMITTING时，能不能有非空running存在？
    if (this->running_tx == nullptr) {
        this->running_tx = new Transaction();
        this->running_tx->tx_state = TX_RUNNING;
    }
    this->running_tx->ref_count++;
    Transaction* tx = this->running_tx;
//    LOG(INFO) << "StartTx: " << this->running_tx->ref_count;

    pthread_mutex_unlock(&this->journal_tx_mutex);  // journal_tx_mutex应该是保护了running_tx, committing_tx等所有tx状态
    return tx;
}

/**
 * page can't be lock when use StopTx, may give deadlock since it need to WaitForCommitDone
 * @param tx
 */
void Journal::StopTx(Transaction* tx) {
    pthread_mutex_lock(&this->journal_tx_mutex);
    tx->ref_count--;    // 初始化tx时ref_count++，跟这个对称。唯一的一对tx->ref_count++/--
    if (tx->ref_count <= 0) {
        //notify commit thread to start commit (if the commit thread is waiting)
        pthread_cond_signal(&tx->ref_cond);     // ref_cond用于在commit时等待ref_count==0
    }

    // tx的块太多了，需要停下来（等待同步的commit）在放tx继续运行
    if (tx->block_num >= STOP_TX_MAX_BLOCK_NUM && this->commit_tx != nullptr && this->commit_tx->tx_state == TX_COMMITTING) {
        WaitForCommitDone();
    }
//    LOG(INFO) << "StopTx: " << tx->ref_count;

    pthread_mutex_unlock(&this->journal_tx_mutex);

    // 异步提交tx（异步的commit）
    if (tx->block_num >= TX_MAX_BLOCK_NUM) {
        CallCommit();
    }

}

/**
 * page should be locked outside
 * 将一个page加入到tx的page list中，分为page第一次被加入tx，page已经被加入tx但是没有被修改过（要被running tx修改）. 
 * 不会出现page已经被加入tx并且被修改过(committing+running+running三个tx impossible)
 * @param page 要被加入tx的page
 * @param tx    要加入的tx
 */
void Journal::JournalGetWriteAccess(Transaction* tx, std::shared_ptr<Page> page) {
    page->SetTxRunning();   // this->flags |= PAGE_TX_RUNNING;
    if (page->tx == nullptr) {  // todo: page第一次被分配给tx，那一定是一个新的running tx？
        page->tx = tx;
        //page is first appeared in this tx, should be add to page list
        page->SetAddTxList();   // this->flags |= PAGE_ADD_TX_LIST;
    } else if (page->tx != tx && !page->modified) { // page已经被分配给了其他runningtx，但是没有被修改过
        //in commit tx, copy the data to frozen data
        page->frozen_data = (char*)malloc(page->page_size);
        memcpy(page->frozen_data, page->data, page->page_size);
        page->modified = 1; // 该页面属于一个committing tx，但又被一个running tx修改了
        page->next_tx = tx; // 再次修改该页面的running tx
        page->SetAddTxList();   // this->flags |= PAGE_NEED_ADD_TX_LIST;
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

// 将journal super block持久化到objstore
void Journal::FlushJSB() {
    pthread_mutex_lock(&this->jsb_mutex);
    this->objstore->ObjWrite(jsb_oid, (char*)this->jsb, sizeof(JournalSuperBlock), 0);
    pthread_mutex_unlock(&this->jsb_mutex);
}

GCFSErr Journal::CommitUnlock(bool force) {
    LOG(INFO) << __func__ ;
    uint32_t desc_block_max_num, need_block_max_num;
    pthread_mutex_lock(&this->journal_tx_mutex);
    if (this->running_tx == nullptr) {  // 既然要commit,那么running_tx一定不为空
        pthread_mutex_unlock(&this->journal_tx_mutex);
        return GCFSErr_SUCCESS;
    }

    //commit tx must be null, switch the tx
    // 这里就是将running_tx切换为commit_tx：转换前commit tx必须为空，转换后running tx被清空
    assert(this->commit_tx == nullptr);
    this->commit_tx = this->running_tx;
    this->running_tx = nullptr;
    if (commit_tx->ref_count > 0) { // ref_count用于在commit时等待ref_count==0，直到ref_count==0，处于commit等待阶段
        //if the waiting for commiting, new tx can't be started.
        LOG(INFO) << "Commit wait ..." ;
        this->commit_tx->tx_state = TX_WAITTING_COMMIT;
        pthread_cond_wait(&this->commit_tx->ref_cond, &this->journal_tx_mutex);
        this->commit_tx->tx_state = TX_COMMITTING;  // 一旦ref_count==0，就可以开始commit了
        pthread_cond_broadcast(&this->commit_state_cond);
        LOG(INFO) << "Commit wait done" ;
    }
    this->commit_tx->tx_state = TX_COMMITTING;  // ref_count本来就不大于0或者变得不大于0，转换tx_state为TX_COMMITTING

    // 添加脏的超级块页
    if (sbi->sbi_dirty) {
        sbi->sb_page->LockPage(PAGE_WRITE);
        memcpy(sbi->sb_page->data, (char*)&sbi->sb, sizeof(GCFSSuperBlock));
        memcpy(sbi->sb_page->data+(sbi->sb.s_root_inode_off), (char*)&sbi->root_inode.gcfs_inode, sizeof(GCFSInode));
        memcpy(sbi->sb_page->data+(sbi->sb.s_fiq_off), (char*)&sbi->fiq_groups, sizeof(FIQGroups));
        MarkPageDirtyUnlock(this->commit_tx, sbi->sb_page); // 将sbi->sb_page的改动加入commit_tx（元数据的改动），会增加tx->block_num
        sbi->sbi_dirty = false;
        sbi->sb_page->UnlockPage();
    }

    pthread_mutex_unlock(&this->journal_tx_mutex);

    if (this->commit_tx->dirty_pages.empty()) {
        delete commit_tx;   // commit_tx中没有脏页，直接析构commit_tx，并将指针置为空
        this->commit_tx = nullptr;
        return GCFSErr_SUCCESS;
    }

    //desc block need to add header 64B
    desc_block_max_num = ((this->commit_tx->dirty_pages.size()+1) * DESCRITOR_INFO_MAX_SIZE + JOURNAL_BLOCK_SIZE - 1) / JOURNAL_BLOCK_SIZE;
    need_block_max_num = desc_block_max_num + this->commit_tx->block_num + 1; //add commit block

    while (need_block_max_num >= this->jsb->j_free) {
        if (!force) {   // 这里force为false，表示不强制checkpoint，那么就返回空间不足
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
    header->block_type = TX_DESCRIPTOR_BLOCK;   // todo 这个journal header是描述符块？what does it mean?
    header->desc_num = this->commit_tx->dirty_pages.size(); // 脏页的数量
    header->desc_block_num = desc_block_max_num;    // 要跟的描述符占的页数量
    desc_block_off = DESCRITOR_INFO_MAX_SIZE;   // 描述符的大小

    //1. write descritor blocks
    while (true) {  // todo 根据JOURNAL_BLOCK_SIZE对desc_block_off的限制来限制tx的大小?
        //  先把脏页列表中不脏的页给删掉，因为不脏的页不需要写入journal。
        // fixme: 这里有个疑惑是脏页怎么变不脏的
        if (page_iter != this->commit_tx->dirty_pages.end() && !(*page_iter)->Dirty()) {
            this->commit_tx->block_num -= ((*page_iter)->page_size / JOURNAL_BLOCK_SIZE);   // block_num是脏页以4K为计量单位的数目
            this->commit_tx->dirty_pages.erase(page_iter++);    // 脏页列表中去除该脏页(因为它不脏 !(*page_iter)->Dirty, 所以不该出现在脏页列表中)
            continue;
        }

//        LOG(INFO) << "Commit write descritor blocks: " << block_nr;
        // JOURNAL_BLOCK_SIZE = 4KB，desc_block_off不能大于4KB，这个限制使得desc_block要定期刷新
        // todo 那对应的块刷新吗？

        // desc_block写满了一个JOURNAL_BLOCK_SIZE，或者脏页列表遍历完了，刷新desc_block
        if (desc_block_off >= JOURNAL_BLOCK_SIZE || page_iter == this->commit_tx->dirty_pages.end()) {
            JournalGetNextBlock(block_nr);
            if (is_header) {
                header_block_nr = block_nr;
                is_header = false;
            }
            GetJoidAndOff(block_nr, j_oid_idx, off);
            ObjID j_oid = make_journal_block_oid(j_oid_idx);
            this->objstore->ObjWrite(j_oid, desc_block, JOURNAL_BLOCK_SIZE, off);
            desc_block_off = 0;
            // 每次都写一个desc_info，把desc_block_off往前推，数次后就写满了一个desc_block
            // 每写满一个desc_block，刷新desc_block到j_oid
            // 直到所有脏页处理完，离开while循环。这个时候可能刷新了好几个desc_block到objstore
            if (page_iter == this->commit_tx->dirty_pages.end()) break; // 跳出block的方法是判断脏页列表是否遍历完了
        }

//        uint32_t page_block_num = (*page_iter)->page_size / JOURNAL_BLOCK_SIZE;
        DescritorInfo* desc_info = (DescritorInfo*) &desc_block[desc_block_off];
        desc_info->page_len = (*page_iter)->page_size;
        desc_info->off = (*page_iter)->off;
        desc_info->oid_len = sizeof(ObjID);
        memcpy(&(desc_info->oid), &((*page_iter)->oid), desc_info->oid_len);
        desc_block_off += DESCRITOR_INFO_MAX_SIZE;  // 目前收集了一个journal_header和一个desc_info

        page_iter ++;   // 脏页列表的迭代器往前推
    }

//    LOG(INFO) << "Commit write descritor blocks done";
    //recalculate since some not dirty pages were delete
    // 此时，脏页列表遍历完了
    desc_block_max_num = ((this->commit_tx->dirty_pages.size()+1) * DESCRITOR_INFO_MAX_SIZE + JOURNAL_BLOCK_SIZE - 1) / JOURNAL_BLOCK_SIZE;
    need_block_max_num = desc_block_max_num + this->commit_tx->block_num + 1; //add commit block
    // journal总共需要的block数：desc_block数 + 脏页数 + 1个commit block

    commit_tx->data_start_blknr = (block_nr + 1) % JOURNAL_BLOCK_MAX_NUM;
    commit_tx->used_block_num = need_block_max_num;

    header = (JournalHeader*) &desc_block[0];
    header->block_type = TX_DESCRIPTOR_BLOCK;
    header->desc_num = this->commit_tx->dirty_pages.size(); // 脏页列表中不脏的页去掉了，所以这里的脏页数可能改变
    header->desc_block_num = desc_block_max_num;    // 脏页列表中脏页数量更新, desc_block_num也要更新
    GetJoidAndOff(header_block_nr, j_oid_idx, off);
    ObjID j_oid = make_journal_block_oid(j_oid_idx);
    this->objstore->ObjWrite(j_oid, desc_block, sizeof(JournalHeader), off);    // 刷新desc_block到objstore

    //2. flush dirty page block
    for (flush_iter = commit_tx->dirty_pages.begin(); flush_iter != commit_tx->dirty_pages.end(); flush_iter++) {
        if (flush_iter != this->commit_tx->dirty_pages.end() && !(*flush_iter)->Dirty()) {
            LOG(WARNING) << "Commit write blocks, page not dirty at all";   // 这里不应该出现page非dirty的情况
        }
        (*flush_iter)->LockPage(PAGE_WRITE);
        char* flush_data;
        if ((*flush_iter)->modified && (*flush_iter)->frozen_data != nullptr) {
            // page data被running tx modified，使用 frozen_data
            flush_data = (*flush_iter)->frozen_data;
        } else {
            flush_data = (*flush_iter)->data;
        }   // 自此, page data被取出来到flush_data，参考了是否modified从不同的属性去取
        for (int j = 0; j < ((*flush_iter)->page_size / JOURNAL_BLOCK_SIZE); ++j) {
            JournalGetNextBlock(block_nr);
            GetJoidAndOff(block_nr, j_oid_idx, off);
            j_oid = make_journal_block_oid(j_oid_idx);
//            LOG(INFO) << "start ObjWrite: " << "block_nr: " << block_nr << " j_oid_idx: " << j_oid_idx << " off: " << off << " data off: " << j*jsb->j_block_size << " data: " << flush_data[j*jsb->j_block_size];
            this->objstore->ObjWrite(j_oid, flush_data+(j*jsb->j_block_size), jsb->j_block_size, off);
            // 刷新日志就是把日志写到objstore，调用了ObjWrite函数
        }
        if ((*flush_iter)->modified && (*flush_iter)->frozen_data != nullptr) {
            // 该tx是committing tx，且page data被running tx modified，使用 frozen_data, 那么用完清空frozen_data
            (*flush_iter)->modified = 0;
            free((*flush_iter)->frozen_data);
            (*flush_iter)->frozen_data = nullptr;
            (*flush_iter)->tx = this->running_tx;   // 在该page中，当前running_tx从 page->next_tx 上位为 page->tx
            (*flush_iter)->next_tx = nullptr;   // 在该page中，next_tx置空
        } else {    //　该tx是running tx
            (*flush_iter)->ClearTxRunning();
            (*flush_iter)->tx = nullptr;
            (*flush_iter)->next_tx = nullptr;
        }
        (*flush_iter)->UnlockPage(); // 对该page的处理完成了，解锁页面
    }
//    LOG(INFO) << "Commit write data blocks done";

    // 下面是写commit block
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
    // flush journal superblock
    FlushJSB();

    //release obj in release set
    std::set<ReleaseKey>::iterator iter;
    // todo 这个release_obj_set里面的元素是什么时候加进去的？
    pthread_mutex_lock(&this->commit_tx->release_obj_set_mutex);
    iter = this->commit_tx->release_obj_set.begin();
    for (iter = this->commit_tx->release_obj_set.begin(); iter != this->commit_tx->release_obj_set.end(); iter++) {
        this->objstore->ObjRelease(ReleaseKeyToOid(*iter));
    }
    this->commit_tx->release_obj_set.clear();
    pthread_mutex_unlock(&this->commit_tx->release_obj_set_mutex);

    // commit tx完成后，将其加入到checkpoint_tx_list中，并把当前journal的commit_tx指针置空
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

// Commit包装了实际执行commit的CommitUnlock和用于唤醒等待在commit_finish_cond上的线程
GCFSErr Journal::Commit(bool force) {
    GCFSErr res = GCFSErr_SUCCESS;
    pthread_mutex_lock(&this->commit_mutex);
    res = CommitUnlock(force);
    pthread_mutex_unlock(&this->commit_mutex);
    //notify finish
    pthread_mutex_lock(&this->commit_finish_mutex);
    pthread_cond_broadcast(&this->commit_finish_cond);  // todo哪些线程等待在commit_finish_cond上？
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
    if (checkpoint_tx_list.empty()) {   // 没有需要checkpoint的tx, 解锁cp_tx_list并返回
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
            // 该page是脏的，应用的时候要从磁盘（ojb store）读旧版本
            // we need to read the old version of the page from the journal, page is still dirty
            uint32_t j_oid_idx, off, read_size=0;
            // todo temp_data用于拷贝当前page的数据？当它比page小时，就重新分配一个大的内存
            if (last_temp_page_size < page->page_size) {
                if (temp_data != nullptr) free(temp_data);
                temp_data = (char*) malloc(page->page_size);
                last_temp_page_size = page->page_size;
            }
            GetJoidAndOff(cur_journal_blknr, j_oid_idx, off);
            while (read_size < page->page_size) {
                // OBJECT_SIZE-off: 当前journal object中剩余的空间
                // page->page_size-read_size: 当前page中未读的空间
                // cur_read_size: 当前读的大小，取上述两个值中较小的一个
                uint32_t cur_read_size = (OBJECT_SIZE-off) < (page->page_size-read_size) ? (OBJECT_SIZE-off) : (page->page_size-read_size);
                ObjID j_oid = make_journal_block_oid(j_oid_idx);    // make_journal_block_oid: 从j_oid_idx生成j_oid
                this->objstore->ObjRead(j_oid, temp_data+read_size, cur_read_size, off);
                j_oid_idx = (j_oid_idx + 1) % JOURNAL_OBJECT_MAX_NUM;
                off = 0;
                read_size += cur_read_size;
            }
            flush_data = temp_data;
        } else {    // page不是脏的，checkpoint的时候直接应用page的数据（page是内存中的页，相对应的，上面的obj store被认为是磁盘上的块）
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
    this->jsb->j_tail = (this->jsb->j_tail + cp_tx->used_block_num) % JOURNAL_BLOCK_MAX_NUM;    // checkpoint是应用了日志，所以j_tail要往前跟进
    this->jsb->j_free += cp_tx->used_block_num;
    // journal super block的j_tail和j_free要更新到objstore里面
    this->objstore->ObjWrite(jsb_oid, (char*)this->jsb, sizeof(JournalSuperBlock), 0);
    pthread_mutex_unlock(&this->jsb_mutex);

    pthread_mutex_lock(&this->cp_tx_list_mutex);
    checkpoint_tx_list.pop_front();
    pthread_mutex_unlock(&this->cp_tx_list_mutex);

    if (temp_data != nullptr) free(temp_data);
    delete cp_tx;
    LOG(INFO) << "Checkpoint done";
}

// 类似commit，干两件事：真正的checkpoint和广播checkpoint完成的条件变量
// todo: 谁在等这些条件变量？
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

// optimize看到这里了
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
        GetJoidAndOff(jsb->j_tail, j_oid_idx, off);   // 从块编号jsb->j_tail中获取块落在哪个oid里面j_oid_idx和块在oid里面的偏移量off
        ObjID j_oid = make_journal_block_oid(j_oid_idx);    // make_journal_block_oid: 从索引j_oid_idx生成ObjID对象j_oid
        objstore->ObjRead(j_oid, (char*)&journal_header, sizeof(JournalHeader), off);   // 先读一个JournalHeader大小的数据，得到journal_header
        total_size = journal_header.desc_num*DESCRITOR_INFO_MAX_SIZE;

        if (last_desc_buf_size < total_size) {  // 分配一个能容纳total_size的desc_buf
            if (desc_buf != nullptr) free(desc_buf);
            desc_buf = (char*) malloc(total_size);
            last_desc_buf_size = total_size;
        }
//        char* desc_buf = (char*)malloc(total_size);
        read_size = 0;
        off += DESCRITOR_INFO_MAX_SIZE; // DESCRIPTOR_INFO_MAX_SIZE是JournalHeader的大小（4个uint32_t，应该是16字节？），所以off要跳过JournalHeader
        while (read_size < total_size) {
            // 461行的ckeckpoint也有类似的代码，当前obj剩余的大小如果小于该页尚未读的大小，就先只读obj剩余的大小。cur_read_size是这次读的大小
            uint32_t cur_read_size = (OBJECT_SIZE-off) < (total_size-read_size) ? (OBJECT_SIZE-off) : (total_size-read_size);
            j_oid = make_journal_block_oid(j_oid_idx);
            objstore->ObjRead(j_oid, desc_buf+read_size, cur_read_size, off);
            j_oid_idx = (j_oid_idx + 1) % JOURNAL_OBJECT_MAX_NUM;   // 如果当前j_oid_idx是journal中的最后一个object，就回到journal另一端重新读（journal是环形的）
            off = 0;
            read_size += cur_read_size;
        }
        jsb->j_tail = (jsb->j_tail + journal_header.desc_block_num) % JOURNAL_BLOCK_MAX_NUM;    // jsb->j_tail也要往前推，过了最大值就回环过来
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
    // 所有的journal recovery完成了，journal现在是空的了
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

// gettimeofday获取timeval结构表示的时间，然后把这个时间转换为timespec结构。unix环境高级编程有这个例子
void get_cond_wait_abstime(struct timespec& out_abstime, const int time_interval) {
    struct timeval now;
    gettimeofday(&now, nullptr);
    // now.tv_usec * 1000: 把微秒转换为纳秒
    // time_interval % 1000 * 1000000: 把毫秒转换为纳秒,time_interval末三位是毫秒为单位，也即他自己就是毫秒单位的
    // nsec: 现在时刻的微秒部分和time_interval的毫秒部分相加，得到纳秒单位的时间
    long nsec = now.tv_usec * 1000 + (time_interval % 1000) * 1000000;
    // noew.tv_sec: 现在时刻的秒部分；nsec: 现在时刻的微秒部分和时间间隔的毫秒部分相加得到的纳秒部分->秒部分；时间间隔秒部分
    // 加起来得到要等待的未来时刻的秒部分
    out_abstime.tv_sec = now.tv_sec + nsec / 1000000000 + time_interval / 1000;
    out_abstime.tv_nsec = nsec % 1000000000;    // 现在时刻的微秒部分和时间间隔的毫秒部分相加得到的纳秒部分->纳秒部分
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

// WaitForCommitDone在commit_finish_mutex上等待commit_finish_cond
void Journal::WaitForCommitDone() {
    pthread_mutex_lock(&this->commit_finish_mutex);
    pthread_cond_wait(&this->commit_finish_cond, &this->commit_finish_mutex);
    pthread_mutex_unlock(&this->commit_finish_mutex);
}

static void* checkpoint_thread_func(void * args) {
    Journal* journal = (Journal*) args;
    journal->PeriodCheckpoint();
}

// todo: checkpoint_thread_func的参数args*怎么传进去的？
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
        // 获取timespec格式的绝对时间，给pthread_cond_timedwait()函数使用
        get_cond_wait_abstime(out_abstime, kCheckPointTimeIntervalMs);
        pthread_mutex_lock(&this->checkpoint_start_mutex);
        if (!this->checkpoint_start_flag)
            // pthread_cond_timedwait()函数的第三个参数是一个绝对时间，const struct timespec *restrict tsptr
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

// pthread_cond_signal通知线程条件已满足，至少能唤醒一个等待该条件的线程。
// （pthread_cond_broadcast则能唤醒等待该条件的所有线程）
void Journal::CallCheckpoint(bool capacity_emergent) {
    pthread_mutex_lock(&this->checkpoint_start_mutex);
    if (capacity_emergent) this->journal_or_pc_capacity_emergent = true;
    this->checkpoint_start_flag = true;
    pthread_cond_signal(&this->checkpoint_start_cond);
    pthread_mutex_unlock(&this->checkpoint_start_mutex);
}

// 我们使用pthread_cond_wait等待条件变量变为真。传递给pthread_cond_wait的互斥量对条件进行保护。
// 调用者把锁住的互斥量传递给函数，函数然后自动把调用线程放到等待条件的线程列表上，对互斥量解锁。
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
