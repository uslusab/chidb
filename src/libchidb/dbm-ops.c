/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine operations.
 *
 */

/*
 *  Copyright (c) 2009-2015, The University of Chicago
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or withsend
 *  modification, are permitted provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  - Neither the name of The University of Chicago nor the names of its
 *    contributors may be used to endorse or promote products derived from this
 *    software withsend specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY send OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *
 */


#include "dbm.h"
#include "btree.h"
#include "record.h"

#include "chidb/log.h"


/* Function pointer for dispatch table */
typedef int (*handler_function)(chidb_stmt *stmt, chidb_dbm_op_t *op);

/* Single entry in the instruction dispatch table */
struct handler_entry
{
    opcode_t opcode;
    handler_function func;
};

/* This generates all the instruction handler prototypes. It expands to:
 *
 * int chidb_dbm_op_OpenRead(chidb_stmt *stmt, chidb_dbm_op_t *op);
 * int chidb_dbm_op_OpenWrite(chidb_stmt *stmt, chidb_dbm_op_t *op);
 * ...
 * int chidb_dbm_op_Halt(chidb_stmt *stmt, chidb_dbm_op_t *op);
 */
#define HANDLER_PROTOTYPE(OP) int chidb_dbm_op_## OP (chidb_stmt *stmt, chidb_dbm_op_t *op);
FOREACH_OP(HANDLER_PROTOTYPE)


/* Ladies and gentlemen, the dispatch table. */
#define HANDLER_ENTRY(OP) { Op_ ## OP, chidb_dbm_op_## OP},

struct handler_entry dbm_handlers[] =
        {
                FOREACH_OP(HANDLER_ENTRY)
        };

int chidb_dbm_op_handle (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    return dbm_handlers[op->opcode].func(stmt, op);
}


/*** INSTRUCTION HANDLER IMPLEMENTATIONS ***/


int chidb_dbm_op_Noop (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    return CHIDB_OK;
}

int openCursor (chidb_stmt *stmt, chidb_dbm_op_t *op, chidb_dbm_cursor_type_t type) {
    // p1: cursor number c
    // p2: register containing page number
    // p3: number of columns in the table, 0 if index
    int32_t c = op->p1;
    chidb_dbm_cursor_t* cur = &stmt->cursors[c];
    chidb_dbm_register_t r1 = stmt->reg[op->p2];
    npage_t pageno = r1.value.i;
    int32_t ncols = op->p3;
    if (!EXISTS_CURSOR(stmt, c)) {
        realloc_cur(stmt, c-1);
    }
    int rc = chidb_dbm_cursor_init(cur, type, stmt->db->bt, pageno);
    if (rc != CHIDB_OK) {
        return rc;
    }
    return CHIDB_OK;
}

int chidb_dbm_op_OpenRead (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    return openCursor(stmt, op, CURSOR_READ);
}


int chidb_dbm_op_OpenWrite (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    return openCursor(stmt, op, CURSOR_WRITE);
}


int chidb_dbm_op_Close (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c = op->p1;
    chidb_dbm_cursor_t* cur = &stmt->cursors[c];
    int rc = chidb_dbm_cursor_free(cur);
    if (rc != CHIDB_OK) {
        return rc;
    }
    return CHIDB_OK;
}


int chidb_dbm_op_Rewind (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    int32_t c = op->p1;
    int32_t j = op->p2;
    chidb_dbm_cursor_t* cur = &stmt->cursors[c];

    // TODO: move this block into rewind, add error code to rewind
    // If btree is empty, jump to address j
    BTreeNode* root_btn = cur->nodes[0];
    if (root_btn->n_cells == 0) {
        stmt->pc = j;
        return CHIDB_OK;
    }

    return chidb_dbm_cursor_rewind(cur);
}


int chidb_dbm_op_Next (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c = op->p1;
    int32_t j = op->p2;
    chidb_dbm_cursor_t* cur = &stmt->cursors[c];

    //chilog(ERROR, "Got next with c=%d, j=%d", c, j);
    int rc = chidb_dbm_cursor_next(cur);
    // only jump if we DID move to a next
    if (rc != CHIDB_OK && rc != CHIDB_CURSOR_ENONEXT) {
        chilog(ERROR, "Some other error with next! rc=%d", rc);
        return rc;
    }
    else if (rc == CHIDB_CURSOR_ENONEXT) {
        // do nothing
    }
    else if (rc == CHIDB_OK) {
        // We moved to a next entry, so also jump
        stmt->pc = j;
    }
    else {
        // idk what you want from me
        chilog(ERROR, "Should never happen");
    }
    return CHIDB_OK;
}


int chidb_dbm_op_Prev (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Seek (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */
    int32_t c = op->p1;
    int32_t j = op->p2;
    int32_t key = stmt->reg[op->p3].value.i;
    chidb_dbm_cursor_t* cur = &stmt->cursors[c];

    int rc = chidb_dbm_cursor_seek(cur, (chidb_key_t)key);
    if (rc == CHIDB_CURSOR_EKEYNOTFOUND) {
        stmt->pc = j;
    }

    return CHIDB_OK;
}


int chidb_dbm_op_SeekGt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c = op->p1;
    int32_t j = op->p2;
    int32_t key = stmt->reg[op->p3].value.i;
    chidb_dbm_cursor_t* cur = &stmt->cursors[c];

    int rc = chidb_dbm_cursor_seekgt(cur, (chidb_key_t)key);
    if (rc == CHIDB_CURSOR_EKEYNOTFOUND) {
        stmt->pc = j;
    }

    return CHIDB_OK;
}


int chidb_dbm_op_SeekGe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c = op->p1;
    int32_t j = op->p2;
    int32_t key = stmt->reg[op->p3].value.i;
    chidb_dbm_cursor_t* cur = &stmt->cursors[c];

    int rc = chidb_dbm_cursor_seekge(cur, (chidb_key_t)key);
    if (rc == CHIDB_CURSOR_EKEYNOTFOUND) {
        stmt->pc = j;
    }

    return CHIDB_OK;
}

int chidb_dbm_op_SeekLt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    int32_t c = op->p1;
    int32_t j = op->p2;
    int32_t key = stmt->reg[op->p3].value.i;
    chidb_dbm_cursor_t* cur = &stmt->cursors[c];

    int rc = chidb_dbm_cursor_seekge(cur, (chidb_key_t)key);
    if (rc == CHIDB_CURSOR_EKEYNOTFOUND) {
        stmt->pc = j;
    }

    return CHIDB_OK;

    return CHIDB_OK;
}


int chidb_dbm_op_SeekLe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}

int chidb_dbm_op_Column (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Key (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Integer (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    // Store p1 in register p2
    chidb_dbm_register_t* r = &stmt->reg[op->p2];
    r->type = REG_INT32;
    r->value.i = op->p1;
    return CHIDB_OK;
}


int chidb_dbm_op_String (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    // Store p4 in register p2 with length p1
    chidb_dbm_register_t* r = &stmt->reg[op->p2];
    r->type = REG_STRING;
   // r->value.s = strndup(op->p4, op->p1);
    return CHIDB_OK;
}


int chidb_dbm_op_Null (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    // Store null in register p2
    chidb_dbm_register_t* r = &stmt->reg[op->p2];
    r->type = REG_NULL;

    return CHIDB_OK;
}


int chidb_dbm_op_ResultRow (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_MakeRecord (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Insert (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}

// return < 0 if r1 < r2, 0 if r1 == r2, > 0 if r1 > r2
int chidb_dbm_cmp (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    chidb_dbm_register_t* r1 = &stmt->reg[op->p1];
    chidb_dbm_register_t* r2 = &stmt->reg[op->p3];

    // undefined behavior if either reg is NULL, let's just pretend equal
    if (r1->type == REG_NULL || r2->type == REG_NULL) {
        return 0;
    }

    // assume both regs are the same type (this is what the spec says)
    switch (r1->type) {
        case REG_BINARY:
            if (r1->value.bin.nbytes <= r2->value.bin.nbytes) {
                return memcmp(r2->value.bin.bytes,
                              r1->value.bin.bytes,
                              r1->value.bin.nbytes);
            } else
            if (r1->value.bin.nbytes > r2->value.bin.nbytes) {
                return memcmp(r2->value.bin.bytes,
                              r1->value.bin.bytes,
                              r2->value.bin.nbytes);
            }
            break;
        case REG_INT32:
            return r2->value.i - r1->value.i;
            break;
        case REG_STRING:
            return strcmp(r2->value.s, r1->value.s);
            break;
        case REG_UNSPECIFIED:
            return 0;
            break;
        default:
            return 0;
    }
    return 0;
}

int chidb_dbm_op_Eq (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    // addr to jump to, if r1 matches r2
    uint32_t j = op->p2;

    int cmp = chidb_dbm_cmp(stmt, op);

    if (cmp == 0) {
        stmt->pc = j;
    }

    return CHIDB_OK;
}


int chidb_dbm_op_Ne (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    // addr to jump to, if r1 matches r2
    uint32_t j = op->p2;

    int cmp = chidb_dbm_cmp(stmt, op);

    if (cmp != 0) {
        stmt->pc = j;
    }

    return CHIDB_OK;
}


int chidb_dbm_op_Lt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    // addr to jump to, if r1 matches r2
    uint32_t j = op->p2;

    int cmp = chidb_dbm_cmp(stmt, op);

    if (cmp < 0) {
        stmt->pc = j;
    }

    return CHIDB_OK;
}


int chidb_dbm_op_Le (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    // addr to jump to, if r1 matches r2
    uint32_t j = op->p2;

    int cmp = chidb_dbm_cmp(stmt, op);

    if (cmp <= 0) {
        stmt->pc = j;
    }

    return CHIDB_OK;
}


int chidb_dbm_op_Gt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    // addr to jump to, if r1 matches r2
    uint32_t j = op->p2;

    int cmp = chidb_dbm_cmp(stmt, op);

    if (cmp > 0) {
        stmt->pc = j;
    }

    return CHIDB_OK;
}


int chidb_dbm_op_Ge (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    // addr to jump to, if r1 matches r2
    uint32_t j = op->p2;

    int cmp = chidb_dbm_cmp(stmt, op);

    if (cmp >= 0) {
        stmt->pc = j;
    }

    return CHIDB_OK;
}


/* IdxGt p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 *
 * if (idxkey at cursor p1) > k, jump
 */
int chidb_dbm_op_IdxGt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    fprintf(stderr,"todo: chidb_dbm_op_IdxGt\n");
    exit(1);
}

/* IdxGe p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 *
 * if (idxkey at cursor p1) >= k, jump
 */
int chidb_dbm_op_IdxGe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    fprintf(stderr,"todo: chidb_dbm_op_IdxGe\n");
    exit(1);
}

/* IdxLt p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 *
 * if (idxkey at cursor p1) < k, jump
 */
int chidb_dbm_op_IdxLt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    fprintf(stderr,"todo: chidb_dbm_op_IdxLt\n");
    exit(1);
}

/* IdxLe p1 p2 p3 *
 *
 * p1: cursor
 * p2: jump addr
 * p3: register containing value k
 *
 * if (idxkey at cursor p1) <= k, jump
 */
int chidb_dbm_op_IdxLe (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    fprintf(stderr,"todo: chidb_dbm_op_IdxLe\n");
    exit(1);
}


/* IdxPKey p1 p2 * *
 *
 * p1: cursor
 * p2: register
 *
 * store pkey from (cell at cursor p1) in (register at p2)
 */
int chidb_dbm_op_IdxPKey (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    fprintf(stderr,"todo: chidb_dbm_op_IdxKey\n");
    exit(1);
}

/* IdxInsert p1 p2 p3 *
 *
 * p1: cursor
 * p2: register containing IdxKey
 * p2: register containing PKey
 *
 * add new (IdkKey,PKey) entry in index BTree pointed at by cursor at p1
 */
int chidb_dbm_op_IdxInsert (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    fprintf(stderr,"todo: chidb_dbm_op_IdxInsert\n");
    exit(1);
}


int chidb_dbm_op_CreateTable (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_CreateIndex (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Copy (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_SCopy (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    /* Your code goes here */

    return CHIDB_OK;
}


int chidb_dbm_op_Halt (chidb_stmt *stmt, chidb_dbm_op_t *op)
{
    stmt->pc = stmt->nOps;
    return op->p1;
}
