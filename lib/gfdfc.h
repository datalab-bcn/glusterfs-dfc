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

#ifndef __GFDFC_H__
#define __GFDFC_H__

#define DFC_XATTR_UUID   "trusted.dfc.uuid"
#define DFC_XATTR_ID     "trusted.dfc.id"
#define DFC_XATTR_SORT   "trusted.dfc.sort"
#define DFC_XATTR_OFFSET "trusted.dfc.offset"
#define DFC_XATTR_SIZE   "trusted.dfc.size"

#define DFC_CHILD_DOWN      0
#define DFC_CHILD_STARTING  1
#define DFC_CHILD_PREPARING 2
#define DFC_CHILD_STOPPING  3
#define DFC_CHILD_UP        4
#define DFC_CHILD_FAILED    5

struct _dfc_sort;
typedef struct _dfc_sort dfc_sort_t;

struct _dfc_request;
typedef struct _dfc_request dfc_request_t;

struct _dfc_transaction;
typedef struct _dfc_transaction dfc_transaction_t;

struct _dfc_child;
typedef struct _dfc_child dfc_child_t;

struct _dfc;
typedef struct _dfc dfc_t;

struct _dfc_sort
{
    void *  head;
    size_t  size;
    bool    pending;
    uint8_t data[4096];
};

struct _dfc_request
{
    struct list_head list;
    dfc_child_t *    child;
    call_frame_t *   frame;
    dfc_sort_t       sort;
};

struct _dfc_transaction
{
    sys_mutex_t         lock;
    struct list_head    list;
    dfc_transaction_t * root;
    int64_t             id;
    int64_t             subtxn;
    dfc_t *             dfc;
    uint64_t            group;
    uint64_t            sorted;
    uint64_t            mask;
    uint64_t            extra;
    uint32_t            state;
    inode_t *           inode;
    dfc_sort_t          sort;
    uint64_t            seqs[];
};

struct _dfc_child
{
    sys_lock_t       lock;
    struct list_head list;
    dfc_t *          dfc;
    xlator_t *       xl;
    int64_t          seq;
    int32_t          idx;
    int32_t          state;
    uint32_t         count;
    uint32_t         active;
    struct list_head pool;
    dfc_sort_t *     sort;
};

struct _dfc
{
    sys_mutex_t        lock;
    uuid_t             uuid;
    xlator_t *         xl;
    loc_t              root_loc;
    int64_t            current_txn;
    int64_t            txn_mask;
    uint32_t           max_requests;
    uint32_t           requests;
    uint32_t           count;
    uint32_t           active;
    struct list_head   children;
    struct list_head * txns;
    call_frame_t *     root_frame;
    void            (* notify)(dfc_t *, xlator_t *, int32_t);
};

enum gfdfc_mem_types
{
    gfdfc_mt_dfc_t = sys_mt_end + 1,
    gfdfc_mt_dfc_child_t,
    gfdfc_mt_dfc_transaction_t,
    gfdfc_mt_dfc_request_t,
    gfdfc_mt_dfc_sort_t
};

err_t dfc_initialize(xlator_t * xl, uint32_t max_requests, uint32_t requests,
                     void (* notify)(dfc_t *, xlator_t *, int32_t),
                     dfc_t ** dfc);
void dfc_terminate(dfc_t * dfc);
void dfc_start(dfc_t * dfc, xlator_t * xl);
void dfc_stop(dfc_t * dfc, xlator_t * xl);
int32_t dfc_default_notify(dfc_t * dfc, xlator_t * xl, int32_t event,
                           void * data);
err_t dfc_begin(dfc_t * dfc, uint64_t mask, inode_t * inode, dict_t * xdata,
                dfc_transaction_t ** txn);
err_t dfc_attach(dfc_transaction_t * txn, int32_t idx, dict_t ** xdata);
bool dfc_failed(dfc_transaction_t * txn, int32_t count);
bool dfc_complete(dfc_transaction_t * txn);

#endif /* __GFDFC_H__ */
