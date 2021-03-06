/*
  Copyright (c) 2012-2013 DataLab, S.L. <http://www.datalab.es>

  This file is part of the DFC translator for GlusterFS.

  The DFC translator for GlusterFS is free software: you can redistribute
  it and/or modify it under the terms of the GNU General Public License
  as published by the Free Software Foundation, either version 3 of the
  License, or (at your option) any later version.

  The DFC translator for GlusterFS is distributed in the hope that it will
  be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
  of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with the DFC translator for GlusterFS. If not, see
  <http://www.gnu.org/licenses/>.
*/

#include "gfsys.h"

#include "gfdfc.h"

void dfc_sort_initialize(dfc_sort_t * sort)
{
    sort->head = sort->data;
    sort->size = sizeof(sort->data);
    sort->pending = true;
}

void dfc_sort_destroy(dfc_sort_t * sort)
{
    SYS_FREE(sort);
}

err_t dfc_sort_create(dfc_sort_t ** sort)
{
    dfc_sort_t * tmp;

    SYS_MALLOC(
        &tmp, gfdfc_mt_dfc_sort_t,
        E(),
        RETERR()
    );

    dfc_sort_initialize(tmp);

    *sort = tmp;

    return 0;
}

err_t __dfc_attach(dfc_t * dfc, int64_t id, int64_t seq, void * data,
                   size_t size, dict_t ** xdata)
{
    int64_t txn_ids[2];

    SYS_CALL(
        sys_dict_set_uuid, (xdata, DFC_XATTR_UUID, dfc->uuid, NULL),
        E(),
        RETERR()
    );

    txn_ids[0] = hton64(id);
    txn_ids[1] = hton64(seq);
    SYS_CALL(
        sys_dict_set_bin, (xdata, DFC_XATTR_ID, txn_ids, sizeof(txn_ids),
                           NULL),
        E(),
        RETERR()
    );

    if (data != NULL)
    {
        SYS_CALL(
            sys_dict_set_bin, (xdata, DFC_XATTR_SORT, data, size, NULL),
            E(),
            RETERR()
        );
    }

/*
    SYS_CALL(
        sys_dict_set_int64, (xdata, DFC_XATTR_TIME, time),
        E(),
        RETERR()
    );
*/
    return 0;
}

void dfc_request_destroy(dfc_request_t * req)
{
    atomic_dec(&req->child->count, memory_order_seq_cst);

    STACK_DESTROY(req->frame->root);
    SYS_FREE(req);
}

err_t dfc_request_create(dfc_child_t * child, dfc_request_t ** req)
{
    dfc_request_t * tmp;
    xlator_t * xl;
    err_t error;

    SYS_MALLOC(
        &tmp, gfdfc_mt_dfc_request_t,
        E(),
        RETERR()
    );

    xl = child->dfc->xl;
    SYS_PTR(
        &tmp->frame, create_frame, (xl, xl->ctx->pool),
        ENOMEM,
        E(),
        GOTO(failed, &error)
    );

    tmp->child = child;
    INIT_LIST_HEAD(&tmp->list);
    dfc_sort_initialize(&tmp->sort);

    atomic_inc(&child->count, memory_order_seq_cst);

    *req = tmp;

    return 0;

failed:
    SYS_FREE(tmp);

    return error;
}

err_t __dfc_sort_send(dfc_child_t * child, loc_t * loc, int64_t txn,
                      int64_t seq, dfc_sort_t * sort);

void dfc_request_free(dfc_request_t * req)
{
    dfc_child_t * child;
    dfc_sort_t sort;

    STACK_RESET(req->frame->root);

    child = req->child;
    if (child->state == DFC_CHILD_UP)
    {
        if (child->active < child->dfc->requests)
        {
            dfc_sort_initialize(&sort);
            __dfc_sort_send(child, &child->dfc->root_loc, 0, child->seq,
                            &sort);
        }
        else if ((child->count < child->dfc->max_requests) ||
                 list_empty(&child->pool))
        {
            list_add_tail(&req->list, &child->pool);
        }
        else
        {
            dfc_request_destroy(req);
        }
    }
    else
    {
        dfc_request_destroy(req);
    }
}

void dfc_transaction_destroy(dfc_transaction_t * txn)
{
    sys_mutex_lock(&txn->dfc->lock);

    list_del_init(&txn->list);

    sys_mutex_unlock(&txn->dfc->lock);

    if (txn->inode != NULL)
    {
        inode_unref(txn->inode);
    }

    SYS_FREE(txn);
}

SYS_LOCK_DECLARE(dfc_sort_send, ((dfc_child_t *, child),
                                 (loc_t, loc, PTR, sys_loc_acquire,
                                                   sys_loc_release),
                                 (int64_t, txn),
                                 (int64_t, seq),
                                 (dfc_sort_t *, sort)));

SYS_ASYNC_CREATE(dfc_transaction_extra, ((dfc_t *, dfc),
                                         (uint64_t, mask),
                                         (dfc_transaction_t *, txn)))
{
    loc_t loc;
    dfc_child_t * child;
    int32_t i;

    memset(&loc, 0, sizeof(loc));
    loc.inode = txn->inode;
    i = 0;

    sys_mutex_lock(&dfc->lock);

    list_for_each_entry(child, &dfc->children, list)
    {
        if (mask & 1)
        {
            SYS_LOCK(
                &child->lock,
                dfc_sort_send, (child, &loc, txn->id, txn->seqs[i],
                                NULL)
            );
            mask ^= 1;
            if (mask == 0)
            {
                break;
            }
        }
        mask >>= 1;
        i++;
    }

    sys_mutex_unlock(&dfc->lock);
}

dfc_transaction_t * dfc_txn_lookup(dfc_t * dfc, int64_t num)
{
    dfc_transaction_t * txn;
    struct list_head * item;
    int64_t id;

    id = num & dfc->txn_mask;
    item = dfc->txns[id].next;
    txn = NULL;
    while (item != &dfc->txns[id])
    {
        txn = list_entry(item, dfc_transaction_t, list);
        if (txn->id >= num)
        {
            if (txn->id != num)
            {
                txn = NULL;
            }
            break;
        }
        item = item->next;
    }

    return txn;
}

void dfc_txn_insert(dfc_t * dfc, dfc_transaction_t * txn)
{
    dfc_transaction_t * tmp;
    struct list_head * item;
    int64_t id;

    id = txn->id & dfc->txn_mask;
    item = dfc->txns[id].prev;
    while (item != &dfc->txns[id])
    {
        tmp = list_entry(item, dfc_transaction_t, list);
        if (tmp->id <= txn->id)
        {
            break;
        }
        item = item->prev;
    }

    list_add(&txn->list, item);
}

err_t dfc_transaction_create(dfc_t * dfc, uint64_t mask, inode_t * inode,
                             dict_t * xdata, dfc_transaction_t ** txn)
{
    dfc_transaction_t * tmp, * aux;
    dfc_child_t * child;
    int64_t txn_ids[2];
    uint64_t bits;
    size_t len;
    int32_t i;
    err_t error;

    SYS_ALLOC(
        &tmp,
        sizeof(dfc_transaction_t) + dfc->count * sizeof(uint64_t),
        gfdfc_mt_dfc_transaction_t,
        E(),
        RETERR()
    );

    tmp->dfc = dfc;
    tmp->mask = mask;
    tmp->sorted = 0;
    tmp->state = sys_bits_count64(mask);
    tmp->state |= tmp->state << 16;

    sys_mutex_lock(&dfc->lock);

    len = sizeof(txn_ids);
    if (sys_dict_get_bin(xdata, DFC_XATTR_ID, txn_ids, &len) == 0)
    {
        SYS_TEST(
            len == sizeof(txn_ids),
            EINVAL,
            E(),
            GOTO(failed, &error)
        );
        aux = dfc_txn_lookup(dfc, ntoh64(txn_ids[0]));
        SYS_TEST(
            aux != NULL,
            ENOENT,
            E(),
            GOTO(failed, &error)
        );
        aux = aux->root;
        tmp->root = aux;
        tmp->id = ++aux->subtxn;
        for (i = 0; i < dfc->count; i++)
        {
            tmp->seqs[i] = aux->seqs[i] | INT64_MIN;
        }
        tmp->inode = inode_ref(aux->inode);

        aux->group |= mask;
        tmp->group = aux->group;
        bits = aux->group & ~mask & ~aux->extra;
        aux->extra |= bits;
        tmp->extra = aux->extra;

        dfc_txn_insert(dfc, tmp);

        dfc_sort_initialize(&tmp->sort);
        sys_buf_set_int64(&tmp->sort.head, &tmp->sort.size, tmp->id);

        sys_mutex_initialize(&tmp->lock);

        if (bits != 0)
        {
            SYS_ASYNC(dfc_transaction_extra, (dfc, bits, tmp));
        }
    }
    else
    {
        tmp->group = mask;
        tmp->extra = 0;
        if (inode != NULL)
        {
            inode = inode_ref(inode);
        }
        tmp->inode = inode;
        dfc->current_txn += 256;
        tmp->subtxn = tmp->id = dfc->current_txn;
        tmp->root = tmp;
        i = 0;
        list_for_each_entry(child, &dfc->children, list)
        {
            if (mask & 1)
            {
                tmp->seqs[i] = ++child->seq;
            }
            else
            {
                tmp->seqs[i] = -1;
            }
            i++;
            mask >>= 1;
        }

        dfc_txn_insert(dfc, tmp);

        dfc_sort_initialize(&tmp->sort);
        sys_buf_set_int64(&tmp->sort.head, &tmp->sort.size, tmp->id);

        sys_mutex_initialize(&tmp->lock);
    }
    sys_mutex_unlock(&dfc->lock);

    *txn = tmp;

    return 0;

failed:
    SYS_FREE(tmp);

    return error;
}

err_t dfc_sort_update(dfc_t * dfc, dfc_sort_t * sort, uuid_t uuid, int64_t txn)
{
    uuid_t * client;
    void * ptr, * top, * aux;
    int64_t current;

    ptr = sort->data;
    top = ptr + sizeof(sort->data) - sort->size;
    __sys_buf_get_int64(&ptr);
    while (ptr < top)
    {
        client = __sys_buf_ptr_uuid(&ptr);
        aux = ptr;
        current = __sys_buf_get_int64(&ptr);

        if (uuid_compare(uuid, *client) == 0)
        {
            if ((uuid_compare(uuid, dfc->uuid) < 0) ^ (current > txn))
            {
                __sys_buf_set_int64(&aux, txn);
            }

            return 0;
        }
    }

    SYS_CALL(
        sys_buf_check, (&sort->size, sizeof(uuid_t) + sizeof(int64_t)),
        E(),
        RETERR()
    );

    __sys_buf_set_uuid(&sort->head, uuid);
    __sys_buf_set_int64(&sort->head, txn);

    return 0;
}

void dfc_request_send(dfc_t * dfc, uint64_t mask, void * data, size_t size);

err_t dfc_sort_process_one(dfc_t * dfc, dfc_child_t * child, void * data,
                           size_t size)
{
    dfc_transaction_t * txn;
    uuid_t * uuid;
    int64_t num, need;
    err_t error;

    SYS_CALL(
        sys_buf_get_int64, (&data, &size, &num),
        E(),
        RETERR()
    );

    sys_mutex_lock(&dfc->lock);

    txn = dfc_txn_lookup(dfc, num);
    if (txn != NULL)
    {
        txn = txn->root;
    }

    sys_mutex_unlock(&dfc->lock);

    SYS_TEST(
        txn != NULL,
        ENOENT,
        W(),
        RETERR()
    );

    sys_mutex_lock(&txn->lock);

    while (size > 0)
    {
        SYS_CALL(
            sys_buf_ptr_uuid, (&data, &size, &uuid),
            E(),
            GOTO(failed_lock, &error)
        );
        SYS_CALL(
            sys_buf_get_int64, (&data, &size, &need),
            E(),
            GOTO(failed_lock, &error)
        );

        SYS_CALL(
            dfc_sort_update, (dfc, &txn->sort, *uuid, need),
            E(),
            GOTO(failed_lock, &error)
        );
    }

    txn->sorted |= 1ULL << child->idx;

    SYS_TEST(
        size == 0,
        EINVAL,
        E(),
        LOG(E(), "Invalid sort buffer received."),
        GOTO(failed_lock, &error)
    );

    error = 0;

failed_lock:
    sys_mutex_unlock(&txn->lock);

    if ((atomic_dec(&txn->state, memory_order_seq_cst) & 0xFFFF) == 1)
    {
        dfc_request_send(txn->dfc, txn->sorted, txn->sort.data,
                         sizeof(txn->sort.data) - txn->sort.size);
    }

    return error;
}

err_t dfc_sort_process(dfc_t * dfc, dfc_child_t * child, dfc_sort_t * sort)
{
    void * data;
    uint32_t length;

    while (sort->size > 0)
    {
        SYS_CALL(
            sys_buf_ptr_block, (&sort->head, &sort->size, &data, &length),
            E(),
            RETERR()
        );

        dfc_sort_process_one(dfc, child, data, length);
    }

    SYS_TEST(
        sort->size == 0,
        EINVAL,
        E(),
        RETERR()
    );

    return 0;
}

SYS_CBK_CREATE(dfc_sort_recv, data, ((dfc_t *, dfc), (dfc_request_t *, req)))
{
    SYS_GF_WIND_CBK_TYPE(getxattr) * args;
    dfc_sort_t * sort;

    atomic_dec(&req->child->active, memory_order_seq_cst);

    args = (SYS_GF_WIND_CBK_TYPE(getxattr) *)data;
    if (args->op_ret < 0)
    {
        if ((args->op_errno == ENOTCONN) || (args->op_errno == ENODATA) ||
            (req->child->active == 0))
        {
            dfc_stop(dfc, req->child->xl);
        }

        dfc_request_destroy(req);

        return;
    }

    sort = &req->sort;
    sort->head = sort->data;
    sort->size = sizeof(sort->data);
    SYS_CALL(
        sys_dict_get_bin, (args->dict, DFC_XATTR_SORT, sort->data,
                           &sort->size),
        T(),
        GOTO(done)
    );

    SYS_CALL(
        dfc_sort_process, (dfc, req->child, sort),
        E()
    );

done:
    dfc_request_free(req);
}

err_t __dfc_sort_send(dfc_child_t * child, loc_t * loc, int64_t txn,
                      int64_t seq, dfc_sort_t * sort)
{
    dfc_request_t * req;
    dict_t * xdata;
    err_t error;

    if (list_empty(&child->pool))
    {
        SYS_CALL(
            dfc_request_create, (child, &req),
            E(),
            LOG(E(), "Failed to create a request to send DFC sort data."),
            RETERR()
        );
    }
    else
    {
        req = list_entry(child->pool.next, dfc_request_t, list);
        list_del_init(&req->list);
    }

    xdata = NULL;
    SYS_CALL(
        __dfc_attach, (child->dfc, txn, seq, sort->data,
                       sizeof(sort->data) - sort->size, &xdata),
        E(),
        LOG(E(), "Failed to prepare a DFC sort request."),
        GOTO(failed, &error)
    );

    atomic_inc(&child->active, memory_order_seq_cst);
    SYS_IO(sys_gf_getxattr_wind, (req->frame, NULL, child->xl, loc,
                                  DFC_XATTR_SORT, xdata),
           SYS_CBK(dfc_sort_recv, (child->dfc, req)), NULL);

    sys_dict_release(xdata);

    return 0;

failed:
    dfc_request_free(req);

    return error;
}

SYS_LOCK_DEFINE(dfc_sort_send, ((dfc_child_t *, child),
                                (loc_t, loc, PTR, sys_loc_acquire,
                                                  sys_loc_release),
                                (int64_t, txn),
                                (int64_t, seq),
                                (dfc_sort_t *, sort)))
{
    if (sort == NULL)
    {
        sort = child->sort;
    }

    SYS_CALL(
        __dfc_sort_send, (child, loc, txn, seq, sort),
        E(),
        GOTO(failed)
    );

    if (child->sort == sort)
    {
        dfc_sort_initialize(sort);
    }
    else
    {
        SYS_FREE(sort);
    }

failed:
    SYS_UNLOCK(&child->lock);
}

SYS_LOCK_CREATE(dfc_sort_add, ((dfc_child_t *, child), (void *, data),
                               (size_t, size)))
{
    dfc_sort_t * sort;
    err_t error = ENOBUFS;

    sort = child->sort;
    if (sort != NULL)
    {
        error = SYS_CALL(
                    sys_buf_set_block, (&sort->head, &sort->size, data, size),
                    D()
                );
    }

    if (error != 0)
    {
        SYS_CALL(
            dfc_sort_create, (&sort),
            E(),
            LOG(E(), "Cannot allocate buffers for DFC sort."),
            GOTO(failed)
        );

        child->sort = sort;

        SYS_CALL(
            sys_buf_set_block, (&sort->head, &sort->size, data, size),
            E(),
            LOG(E(), "Cannot store data into DFC sort buffers."),
            GOTO(failed)
        );
    }

    if (sort->pending)
    {
        sort->pending = false;
        SYS_LOCK(&child->lock, dfc_sort_send, (child, &child->dfc->root_loc, 0,
                                               child->seq, sort));
    }

failed:
    SYS_UNLOCK(&child->lock);
}

void dfc_request_send(dfc_t * dfc, uint64_t mask, void * data, size_t size)
{
    dfc_child_t * child;

    list_for_each_entry(child, &dfc->children, list)
    {
        if ((mask & 1) != 0)
        {
            SYS_LOCK(&child->lock, dfc_sort_add, (child, data, size));
        }

        mask >>= 1;
    }
}

void dfc_child_destroy(dfc_child_t * child)
{
    dfc_request_t * req;

    while (!list_empty(&child->pool))
    {
        req = list_entry(child->pool.next, dfc_request_t, list);
        list_del_init(&req->list);

        dfc_request_destroy(req);
    }

    SYS_TEST(
        child->count == 0,
        EBUSY,
        E(),
        ASSERT("There are DFC requests being processed")
    );

    SYS_FREE(child);
}

err_t dfc_child_create(dfc_t * dfc, xlator_t * xl, dfc_child_t ** child)
{
    dfc_child_t * tmp;

    SYS_MALLOC(
        &tmp, gfdfc_mt_dfc_child_t,
        E(),
        RETERR()
    );

    sys_lock_initialize(&tmp->lock);

    tmp->dfc = dfc;
    tmp->xl = xl;
    tmp->count = 0;
    tmp->active = 0;
    tmp->seq = 0;
    tmp->idx = dfc->count;
    tmp->state = DFC_CHILD_DOWN;
    INIT_LIST_HEAD(&tmp->list);
    INIT_LIST_HEAD(&tmp->pool);

    tmp->sort = NULL;

    *child = tmp;

    return 0;
}

void dfc_destroy(dfc_t * dfc)
{
    dfc_child_t * child;

    if (dfc->root_frame != NULL)
    {
        STACK_DESTROY(dfc->root_frame->root);
    }

    inode_unref(dfc->root_loc.inode);

    while (!list_empty(&dfc->children))
    {
        child = list_entry(dfc->children.next, dfc_child_t, list);
        list_del_init(&child->list);

        dfc_child_destroy(child);
    }

    sys_mutex_terminate(&dfc->lock);

    if (dfc->txns != NULL)
    {
        SYS_FREE(dfc->txns);
    }
    SYS_FREE(dfc);
}

err_t dfc_create(xlator_t * xl, uint32_t max_requests, uint32_t requests,
                 void (* notify)(dfc_t *, xlator_t *, int32_t), dfc_t ** dfc)
{
    dfc_t * tmp;
    dfc_child_t * child;
    dfc_request_t * req;
    xlator_list_t * list;
    int32_t i;
    err_t error;

    SYS_MALLOC(
        &tmp, gfdfc_mt_dfc_t,
        E(),
        RETERR()
    );

    sys_mutex_initialize(&tmp->lock);

    tmp->xl = xl;
    memset(&tmp->root_loc, 0, sizeof(tmp->root_loc));
    tmp->root_loc.gfid[15] = 1;
    tmp->root_loc.path = "/";
    tmp->root_loc.name = NULL;
    tmp->root_loc.inode = inode_ref(xl->itable->root);
    tmp->max_requests = max_requests;
    tmp->requests = requests;
    INIT_LIST_HEAD(&tmp->children);
    uuid_generate(tmp->uuid);
    tmp->root_frame = NULL;
    tmp->txns = NULL;
    tmp->notify = notify;

    SYS_PTR(
        &tmp->root_frame, create_frame, (xl, xl->ctx->pool),
        ENOMEM,
        E(),
        GOTO(failed, &error)
    );

    SYS_CALLOC(
        &tmp->txns, 1024, gfdfc_mt_dfc_transaction_t,
        E(),
        GOTO(failed, &error)
    );
    for (i = 0; i < 1024; i++)
    {
        INIT_LIST_HEAD(&tmp->txns[i]);
    }
    tmp->txn_mask = 1023;
    tmp->current_txn = 0;

    tmp->count = 0;
    for (list = xl->children; list != NULL; list = list->next)
    {
        SYS_CALL(
            dfc_child_create, (tmp, list->xlator, &child),
            E(),
            GOTO(failed, &error)
        );

        list_add_tail(&child->list, &tmp->children);
        tmp->count++;

        for (i = 0; i < requests; i++)
        {
            SYS_CALL(
                dfc_request_create, (child, &req),
                E(),
                GOTO(failed, &error)
            );

            list_add_tail(&req->list, &child->pool);
        }
    }

    *dfc = tmp;

    return 0;

failed:
    dfc_destroy(tmp);

    return error;
}

err_t dfc_initialize(xlator_t * xl, uint32_t max_requests, uint32_t requests,
                     void (* notify)(dfc_t *, xlator_t *, int32_t),
                     dfc_t ** dfc)
{
    dfc_t * tmp;

    if (xl->itable == NULL)
    {
        SYS_PTR(
            &xl->itable, inode_table_new, (0, xl),
            ENOMEM,
            E(),
            RETERR()
        );
    }

    SYS_CALL(
        dfc_create, (xl, max_requests, requests, notify, &tmp),
        E(),
        RETERR()
    );


    *dfc = tmp;

    return 0;
}

void dfc_terminate(dfc_t * dfc)
{
    dfc_destroy(dfc);
}

SYS_DELAY_CREATE(dfc_start_delayed, ((dfc_t *, dfc), (dfc_child_t *, child)))
{
    sys_mutex_lock(&dfc->lock);

    if (child->state == DFC_CHILD_PREPARING)
    {
        dfc->active++;
        child->state = DFC_CHILD_UP;
        dfc->notify(dfc, child->xl, DFC_CHILD_UP);
    }
    else if (child->state == DFC_CHILD_STOPPING)
    {
        child->state = DFC_CHILD_DOWN;
    }

    sys_mutex_unlock(&dfc->lock);
}

SYS_CBK_CREATE(__dfc_start_cbk, io, ((dfc_t *, dfc), (dfc_child_t *, child)))
{
    SYS_GF_WIND_CBK_TYPE(lookup) * args;
    dfc_sort_t sort;
    int32_t i;

    args = (SYS_GF_WIND_CBK_TYPE(lookup) *)io;

    sys_mutex_lock(&dfc->lock);

    if (child->state == DFC_CHILD_STARTING)
    {
        if (args->op_ret == 0)
        {
            child->state = DFC_CHILD_PREPARING;
            dfc_sort_initialize(&sort);
            for (i = 0; i < dfc->requests; i++)
            {
                SYS_CALL(
                    __dfc_sort_send, (child, &child->dfc->root_loc, 0,
                                      child->seq, &sort),
                    E()
                );
            }
            SYS_DELAY(1000, dfc_start_delayed, (dfc, child));
        }
        else
        {
            logW("Child '%s' failed to start", child->xl->name);

            child->state = DFC_CHILD_FAILED;
        }
    }
    else if (child->state == DFC_CHILD_STOPPING)
    {
        child->state = DFC_CHILD_DOWN;
    }

    sys_mutex_unlock(&dfc->lock);
}

SYS_ASYNC_CREATE(__dfc_start, ((dfc_t *, dfc), (xlator_t *, xl)))
{
    dfc_sort_t sort;
    dfc_child_t * child;
    dict_t * xdata;

    sys_mutex_lock(&dfc->lock);

    list_for_each_entry(child, &dfc->children, list)
    {
        if (child->xl == xl)
        {
            if (child->state == DFC_CHILD_DOWN)
            {
                dfc_sort_initialize(&sort);
                xdata = NULL;
                SYS_CALL(
                    __dfc_attach, (dfc, 0, child->seq, sort.data,
                                   sizeof(sort.data) - sort.size, &xdata),
                    E(),
                    LOG(E(), "Failed to prepare a DFC sort request."),
                    BREAK()
                );
                child->state = DFC_CHILD_STARTING;

                SYS_IO(
                    sys_gf_lookup_wind, (dfc->root_frame, NULL, xl,
                                         &dfc->root_loc, xdata),
                    SYS_CBK(__dfc_start_cbk, (dfc, child)), NULL
                );

                sys_dict_release(xdata);
            }
            break;
        }
    }

    sys_mutex_unlock(&dfc->lock);
}

void dfc_start(dfc_t * dfc, xlator_t * xl)
{
    SYS_ASYNC(__dfc_start, (dfc, xl));
}

void dfc_stop(dfc_t * dfc, xlator_t * xl)
{
    dfc_child_t * child;

    sys_mutex_lock(&dfc->lock);

    list_for_each_entry(child, &dfc->children, list)
    {
        if (child->xl == xl)
        {
            if (child->state == DFC_CHILD_UP)
            {
                child->state = DFC_CHILD_DOWN;
                dfc->notify(dfc, child->xl, DFC_CHILD_DOWN);
            }
            else if ((child->state == DFC_CHILD_STARTING) ||
                     (child->state == DFC_CHILD_PREPARING))
            {
                child->state = DFC_CHILD_STOPPING;
            }
            else if (child->state == DFC_CHILD_FAILED)
            {
                child->state = DFC_CHILD_DOWN;
            }
            break;
        }
    }

    sys_mutex_unlock(&dfc->lock);
}

int32_t dfc_default_notify(dfc_t * dfc, xlator_t * xl, int32_t event,
                           void *data)
{
    if (event == GF_EVENT_CHILD_UP)
    {
        dfc_start(dfc, data);
    }
    else if (event == GF_EVENT_CHILD_DOWN)
    {
        dfc_stop(dfc, data);
    }
    else
    {
        return default_notify(xl, event, data);
    }

    return 0;
}

err_t dfc_begin(dfc_t * dfc, uint64_t mask, inode_t * inode, dict_t * xdata,
                dfc_transaction_t ** txn)
{
    dfc_transaction_t * tmp;

    SYS_CALL(
        dfc_transaction_create, (dfc, mask, inode, xdata, &tmp),
        E(),
        RETERR()
    );

    *txn = tmp;

    return 0;
}

err_t dfc_attach(dfc_transaction_t * txn, int32_t idx, dict_t ** xdata)
{
    if (txn != NULL)
    {
        SYS_CALL(
            __dfc_attach, (txn->dfc, txn->id, txn->seqs[idx], NULL, 0, xdata),
            E(),
            RETERR()
        );
    }

    return 0;
}

bool dfc_failed(dfc_transaction_t * txn, int32_t count)
{
    uint32_t state;

    if (txn == NULL)
    {
        return true;
    }
    state = count | (count << 16);
    state = atomic_sub_return(&txn->state, state, memory_order_seq_cst);
    if ((state >> 16) == 0)
    {
        dfc_transaction_destroy(txn);

        return true;
    }
    if ((state & 0xFFFF) == 0)
    {
        dfc_request_send(txn->dfc, txn->sorted, txn->sort.data,
                         sizeof(txn->sort.data) - txn->sort.size);
    }

    return false;
}

bool dfc_complete(dfc_transaction_t * txn)
{
    if (txn == NULL)
    {
        return true;
    }
    if ((atomic_sub(&txn->state, 0x10000, memory_order_seq_cst) >> 16) == 1)
    {
        dfc_transaction_destroy(txn);

        return true;
    }

    return false;
}
