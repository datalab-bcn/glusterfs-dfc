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

err_t dfc_test_txn(dfc_t * dfc, dict_t ** xdata, dfc_transaction_t ** txn)
{
    err_t error;

    SYS_CALL(
        dfc_begin, (dfc, txn),
        E(),
        RETERR()
    );
    SYS_CALL(
        dfc_attach, (*txn, xdata),
        E(),
        GOTO(failed, &error)
    );

    return 0;

failed:
    dfc_end(*txn, 0);

    return error;
}

#define DFC_TEST_FOP(_fop) \
    SYS_CBK_CREATE(__dfc_test_##_fop##_cbk, io, ((dfc_transaction_t *, txn))) \
    { \
        if (dfc_complete(txn)); \
        { \
            logT("DFC(" #_fop "_cbk): %ld", txn->id); \
            sys_gf_handler_call_##_fop##_unwind(NULL, 0, 0, NULL, NULL, io); \
        } \
    } \
    SYS_ASYNC_CREATE(__dfc_test_##_fop, ((call_frame_t *, frame), \
                                         (xlator_t *, xl), \
                                         SYS_GF_ARGS_##_fop)) \
    { \
        dfc_transaction_t * txn; \
        xlator_list_t * list; \
        int32_t num_childs; \
        SYS_CALL( \
            dfc_test_txn, (xl->private, &xdata, &txn), \
            E(), \
            GOTO(failed) \
        ); \
        logT("DFC(" #_fop "): %ld", txn->id); \
        num_childs = 0; \
        for (list = xl->children; list != NULL; list = list->next) \
        { \
            SYS_IO(sys_gf_##_fop##_wind, (frame, NULL, list->xlator, \
                                       SYS_ARGS_NAMES((SYS_GF_ARGS_##_fop))), \
                   SYS_CBK(__dfc_test_##_fop##_cbk, (txn)), NULL); \
            num_childs++; \
        } \
        dfc_end(txn, num_childs); \
        return; \
    failed: \
        SYS_IO(sys_gf_##_fop##_unwind_error, (frame, EIO, NULL), NULL, NULL); \
    } \
    static int32_t dfc_test_##_fop(call_frame_t * frame, xlator_t * xl, \
                                   SYS_ARGS_DECL((SYS_GF_ARGS_##_fop))) \
    { \
        SYS_ASYNC(__dfc_test_##_fop, (frame, xl, \
                                      SYS_ARGS_NAMES((SYS_GF_ARGS_##_fop)))); \
        return 0; \
    }

/*
static loc_t __loc;
static loc_t * loc = &__loc;

#define DFC_TEST_FOP(_fop) \
    SYS_CBK_CREATE(__dfc_test_##_fop##_cbk, io, ()) \
    { \
        sys_gf_handler_call_##_fop##_unwind(NULL, 0, 0, NULL, NULL, io); \
    } \
    SYS_ASYNC_CREATE(__dfc_test_##_fop, ((call_frame_t *, frame), \
                                         (xlator_t *, xl), \
                                         SYS_GF_ARGS_##_fop)) \
    { \
        SYS_IO(sys_gf_##_fop##_wind, (frame, NULL, FIRST_CHILD(xl), \
                                      SYS_ARGS_NAMES((SYS_GF_ARGS_##_fop))), \
               SYS_CBK(__dfc_test_##_fop##_cbk, ()), NULL); \
        return; \
    } \
    static int32_t dfc_test_##_fop(call_frame_t * frame, xlator_t * xl, \
                                   SYS_ARGS_DECL((SYS_GF_ARGS_##_fop))) \
    { \
        if (strcmp(#_fop, "lookup") == 0) \
        { \
            logI("LOOKUP: path: '%s'", loc->path); \
            logI("LOOKUP: name: '%s'", loc->name); \
            logI("LOOKUP: inode: %p", loc->inode); \
            logI("LOOKUP: parent: %p", loc->parent); \
            logI("LOOKUP: gfid: %02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", \
                loc->gfid[0], loc->gfid[1], loc->gfid[2], loc->gfid[3], \
                loc->gfid[4], loc->gfid[5], loc->gfid[6], loc->gfid[7], \
                loc->gfid[8], loc->gfid[9], loc->gfid[10], loc->gfid[11], \
                loc->gfid[12], loc->gfid[13], loc->gfid[14], loc->gfid[15]); \
            logI("LOOKUP: pargfid: %02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", \
                loc->pargfid[0], loc->pargfid[1], loc->pargfid[2], loc->pargfid[3], \
                loc->pargfid[4], loc->pargfid[5], loc->pargfid[6], loc->pargfid[7], \
                loc->pargfid[8], loc->pargfid[9], loc->pargfid[10], loc->pargfid[11], \
                loc->pargfid[12], loc->pargfid[13], loc->pargfid[14], loc->pargfid[15]); \
            logI("LOOKUP: inode gfid: %02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", \
                loc->inode->gfid[0], loc->inode->gfid[1], loc->inode->gfid[2], loc->inode->gfid[3], \
                loc->inode->gfid[4], loc->inode->gfid[5], loc->inode->gfid[6], loc->inode->gfid[7], \
                loc->inode->gfid[8], loc->inode->gfid[9], loc->inode->gfid[10], loc->inode->gfid[11], \
                loc->inode->gfid[12], loc->inode->gfid[13], loc->inode->gfid[14], loc->inode->gfid[15]); \
            logI("LOOKUP: inode nlookup: %lu", loc->inode->nlookup); \
            logI("LOOKUP: inode ia_type: %u", loc->inode->ia_type); \
        } \
        SYS_ASYNC(__dfc_test_##_fop, (frame, xl, \
                                      SYS_ARGS_NAMES((SYS_GF_ARGS_##_fop)))); \
        if (first) \
        { \
            sleep(1); \
            first = false; \
        } \
        return 0; \
    }
*/

SYS_CBK_CREATE(__dfc_test_lookup_cbk, io, ((dfc_transaction_t *, txn)))
{
    if (dfc_complete(txn))
    {
        sys_gf_handler_call_lookup_unwind(NULL, 0, 0, NULL, NULL, io); \
    }
}

static uint32_t total;

SYS_CBK_CREATE(__dfc_test_init_lookup_cbk, io, ((dfc_t *, dfc)))
{
    if (atomic_dec(&total, memory_order_seq_cst) == 1)
    {
        sys_gf_handler_call_lookup_unwind(NULL, 0, 0, NULL, NULL, io); \
        dfc_initialize(dfc);
    }
}

SYS_ASYNC_CREATE(__dfc_test_lookup, ((call_frame_t *, frame), (xlator_t *, xl),
                                     SYS_GF_ARGS_lookup))
{
    static bool running = false;
    dfc_t * dfc;
    dfc_transaction_t * txn;
    xlator_list_t * list;
    int32_t num_childs;
    err_t error;

    if (!running)
    {
        running = true;
        SYS_CALL(
            dfc_prepare, (xl, 8, 4, loc->inode, &dfc, &xdata),
            E(),
            GOTO(failed, &error)
        );
        total = 1;
        for (list = xl->children; list != NULL; list = list->next)
        {
            atomic_inc(&total, memory_order_seq_cst);
            SYS_IO(sys_gf_lookup_wind, (frame, NULL, list->xlator, loc, xdata),
                   SYS_CBK(__dfc_test_init_lookup_cbk, (dfc)), NULL);
        }
        if (atomic_dec(&total, memory_order_seq_cst) == 1)
        {
            logE("Too fast !!!");
        }
    }
    else
    {
        dfc = xl->private;
        SYS_CALL(
            dfc_test_txn, (xl->private, &xdata, &txn),
            E(),
            GOTO(failed)
        );
        num_childs = 0;
        for (list = xl->children; list != NULL; list = list->next)
        {
            SYS_IO(sys_gf_lookup_wind, (frame, NULL, list->xlator, loc, xdata),
                   SYS_CBK(__dfc_test_lookup_cbk, (txn)), NULL);
            num_childs++;
        }
        dfc_end(txn, num_childs);
    }

    return;

failed:
    SYS_IO(sys_gf_lookup_unwind_error, (frame, EIO, NULL), NULL, NULL);
}

static int32_t dfc_test_lookup(call_frame_t * frame, xlator_t * xl,
                               loc_t * loc, dict_t * xdata)
{
    logT("DFC(lookup)");
    SYS_ASYNC(__dfc_test_lookup, (frame, xl, loc, xdata));
    return 0;
}

DFC_TEST_FOP(access)
DFC_TEST_FOP(create)
DFC_TEST_FOP(entrylk)
DFC_TEST_FOP(fentrylk)
DFC_TEST_FOP(flush)
DFC_TEST_FOP(fsync)
DFC_TEST_FOP(fsyncdir)
DFC_TEST_FOP(getxattr)
DFC_TEST_FOP(fgetxattr)
DFC_TEST_FOP(inodelk)
DFC_TEST_FOP(finodelk)
DFC_TEST_FOP(link)
DFC_TEST_FOP(lk)
DFC_TEST_FOP(mkdir)
DFC_TEST_FOP(mknod)
DFC_TEST_FOP(open)
DFC_TEST_FOP(opendir)
DFC_TEST_FOP(rchecksum)
DFC_TEST_FOP(readdir)
DFC_TEST_FOP(readdirp)
DFC_TEST_FOP(readlink)
DFC_TEST_FOP(readv)
DFC_TEST_FOP(removexattr)
DFC_TEST_FOP(fremovexattr)
DFC_TEST_FOP(rename)
DFC_TEST_FOP(rmdir)
DFC_TEST_FOP(setattr)
DFC_TEST_FOP(fsetattr)
DFC_TEST_FOP(setxattr)
DFC_TEST_FOP(fsetxattr)
DFC_TEST_FOP(stat)
DFC_TEST_FOP(fstat)
DFC_TEST_FOP(statfs)
DFC_TEST_FOP(symlink)
DFC_TEST_FOP(truncate)
DFC_TEST_FOP(ftruncate)
DFC_TEST_FOP(unlink)
DFC_TEST_FOP(writev)
DFC_TEST_FOP(xattrop)
DFC_TEST_FOP(fxattrop)

static int32_t dfc_test_forget(xlator_t * xl, inode_t * inode)
{
    return 0;
}

static int32_t dfc_test_invalidate(xlator_t * xl, inode_t * inode)
{
    return 0;
}

static int32_t dfc_test_release(xlator_t * xl, fd_t * fd)
{
    return 0;
}

static int32_t dfc_test_releasedir(xlator_t * xl, fd_t * fd)
{
    return 0;
}

int32_t init(xlator_t * xl)
{
    SYS_CALL(
        gfsys_initialize, (NULL, false),
        E(),
        RETVAL(-1)
    );

    return 0;
}

int32_t fini(xlator_t * xl)
{
    return 0;
}

SYS_GF_FOP_TABLE(dfc_test);
SYS_GF_CBK_TABLE(dfc_test);

struct volume_options options[] =
{
    { }
};
