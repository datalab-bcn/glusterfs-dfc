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

#ifndef __DFC_H__
#define __DFC_H__

#define DFC_XATTR "trusted.dfc"

#define DFC_XATTR_UUID DFC_XATTR ".uuid"
#define DFC_XATTR_ID   DFC_XATTR ".id"
#define DFC_XATTR_SORT DFC_XATTR ".sort"
#define DFC_XATTR_TIME DFC_XATTR ".time"

enum dfc_mem_types
{
    dfc_mt_dfc_manager_t = sys_mt_end + 1,
    dfc_mt_dfc_client_t,
    dfc_mt_dfc_sort_t,
    dfc_mt_end
};

#endif /* __DFC_H__ */
