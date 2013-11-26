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

#define DFC_XATTR_UUID "trusted.dfc.uuid"
#define DFC_XATTR_ID   "trusted.dfc.id"
#define DFC_XATTR_SORT "trusted.dfc.sort"

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

enum gfdfc_mem_types
{
    gfdfc_mt_dfc_t = sys_mt_end + 1,
    gfdfc_mt_dfc_child_t,
    gfdfc_mt_dfc_transaction_t,
    gfdfc_mt_dfc_request_t,
    gfdfc_mt_dfc_sort_t
};

err_t dfc_prepare(xlator_t * xl, uint32_t max_requests, uint32_t requests,
                  inode_t * inode, dfc_t ** dfc, dict_t ** xdata);
void dfc_initialize(dfc_t * dfc);

err_t dfc_begin(dfc_t * dfc, dfc_transaction_t ** txn);
err_t dfc_attach(dfc_transaction_t * txn, dict_t ** xdata);
void dfc_end(dfc_transaction_t * txn, uint32_t count);
bool dfc_complete(dfc_transaction_t * txn);

#endif /* __GFDFC_H__ */
