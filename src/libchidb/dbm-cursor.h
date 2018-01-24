/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine cursors -- header
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


#ifndef DBM_CURSOR_H_
#define DBM_CURSOR_H_

#include "chidbInt.h"
#include "btree.h"

#define DEFAULT_CURSOR_MAX_DEPTH (5)

#define CHIDB_CURSOR_ENONEXT (1)
#define CHIDB_CURSOR_EKEYNOTFOUND (2)
#define CHIDB_CURSOR_ENOPREV (3)

typedef enum chidb_dbm_cursor_type
{
    CURSOR_UNSPECIFIED,
    CURSOR_READ,
    CURSOR_WRITE
} chidb_dbm_cursor_type_t;

typedef struct chidb_dbm_cursor
{
    BTree* bt;
    chidb_dbm_cursor_type_t type;
    uint8_t depth; // depth of current page
    uint8_t max_depth; // size of cells and nodes, to see if we need to realloc
    // things that need to be freed
    ncell_t* cells; // current cell number at each node, i.e., cells[i] is the current cell for the node at depth i
    BTreeNode** nodes; // nodes in path to root
} chidb_dbm_cursor_t;

/* Cursor function definitions go here */
int chidb_dbm_cursor_init(chidb_dbm_cursor_t* cursor, chidb_dbm_cursor_type_t type, BTree* bt, npage_t nroot);
int chidb_dbm_cursor_free(chidb_dbm_cursor_t* cursor);
int chidb_dbm_cursor_rewind(chidb_dbm_cursor_t* cur);
int chidb_dbm_cursor_next(chidb_dbm_cursor_t* cursor);
int chidb_dbm_cursor_seek(chidb_dbm_cursor_t* cursor, chidb_key_t key);
int chidb_dbm_cursor_seekge(chidb_dbm_cursor_t* cursor, chidb_key_t key);
int chidb_dbm_cursor_seekgt(chidb_dbm_cursor_t* cursor, chidb_key_t key);

#endif /* DBM_CURSOR_H_ */