/*
 *  chidb - a didactic relational database management system
 *
 * This module contains functions to manipulate a B-Tree file. In this context,
 * "BTree" refers not to a single B-Tree but to a "file of B-Trees" ("chidb
 * file" and "file of B-Trees" are essentially equivalent terms).
 *
 * However, this module does *not* read or write to the database file directly.
 * All read/write operations must be done through the pager module.
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


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <chidb/log.h>
#include "chidbInt.h"
#include "btree.h"
#include "record.h"
#include "pager.h"
#include "util.h"


/* Convert big endian to little endian
*
*/
 /*inline */ uint32_t betole(const uint8_t *data) {
    return data[0] << 24 | data[1] << 16 | data[2] << 8 | data[3];
}

/* Open a B-Tree file
 *
 * This function opens a database file and verifies that the file
 * header is correct. If the file is empty (which will happen
 * if the pager is given a filename for a file that does not exist)
 * then this function will (1) initialize the file header using
 * the default page size and (2) create an empty table leaf node
 * in page 1.
 *
 * Parameters
 * - filename: Database file (might not exist)
 * - db: A chidb struct. Its bt field must be set to the newly
 *       created BTree.
 * - bt: An out parameter. Used to return a pointer to the
 *       newly created BTree.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECORRUPTHEADER: Database file contains an invalid header
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_open(const char *filename, chidb *db, BTree **bt)
{
    /* Your code goes here */
    FILE *logger;
    logger = fopen("log.txt", "w+");
    fprintf(logger, "Beginning chidb_Btree_open\n");
    fflush(logger);

    int rc;
    Pager **pager;
    uint8_t header[100];

    *bt = malloc(sizeof(BTree));
    pager = &((*bt)->pager);

    if ((rc = chidb_Pager_open(pager, filename)) != CHIDB_OK) {
        chilog(ERROR, "Failed to open filename %s\n", filename);
        return rc;
    }

    if ((rc = chidb_Pager_readHeader(*pager, header)) == CHIDB_NOHEADER) {

        // Get first page
        npage_t npage;
        MemPage* page;
        // Create new node in page 1
        // we must set page size manually to the default, since it can't have been read from the header
        chidb_Pager_setPageSize(*pager, DEFAULT_PAGE_SIZE);
        rc = chidb_Btree_newNode(*bt, &npage, PGTYPE_TABLE_LEAF);
        if (rc != CHIDB_OK) {
            chilog(ERROR, "Couldn't make first node");
            return rc;
        }
        // Write file header to first 100 bytes
        rc = chidb_Pager_readPage(*pager, npage, &page);
        if (rc != CHIDB_OK) {
            chilog(ERROR, "Couldn't read first page");
            return rc;
        }
        strcpy(page->data, "SQLite format 3");
        put2byte(page->data+0x10, DEFAULT_PAGE_SIZE);
        memcpy(page->data+0x12, "\x01\x01\x00\x40\x20\x20", 6);
        memset(page->data+0x18, 0, 0x64-0x18);
        put4byte(page->data+0x2C, 1);
        put4byte(page->data+0x30, 20000);
        put4byte(page->data+0x38, 1);

        rc = chidb_Pager_writePage(*pager, page);
        if (rc != CHIDB_OK) {
            chilog(ERROR, "Couldn't write first page");
            return rc;
        }
        return rc;
    }

    uint16_t pageSize = header[16]*256 + header[17];
    fprintf(logger, "Page size is %d\n", pageSize);
    fflush(logger);
    chidb_Pager_setPageSize(*pager, pageSize);

    /* Validate header
    *  The following are values in the header which are all constant in chidb,
    *  but which might change in sqlite.  chidb is a subset of sqlite, so we're
    *  not going as far as to fully implement this header format and instead
    *  will just validate it with constant initial values.
    */
    if (strcmp("SQLite format 3", header) != 0) {
        return CHIDB_ECORRUPTHEADER;
    }

    uint32_t fileChangeCounter = betole(&header[0x18]);
    uint32_t schemaVersion = betole(&header[0x28]);
    uint32_t pageCacheSize = betole(&header[0x30]);
    uint32_t userCookie = betole(&header[0x3C]);
    if (
            fileChangeCounter != 0      || // Unused
            betole(&header[0x20]) != 0  || betole(&header[0x24]) != 0 ||
            schemaVersion != 0          || betole(&header[0x2C]) != 1 ||
            pageCacheSize != 20000      || betole(&header[0x34]) != 0 ||
            betole(&header[0x38]) != 1  || userCookie != 0 ||
            betole(&header[0x40]) != 0
            ) {
        return CHIDB_ECORRUPTHEADER;
    }

    return CHIDB_OK;
}


/* Close a B-Tree file
 *
 * This function closes a database file, freeing any resource
 * used in memory, such as the pager.
 *
 * Parameters
 * - bt: B-Tree file to close
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_close(BTree *bt)
{
    /* Your code goes here */
    int rc;
    if ( (rc = chidb_Pager_close(bt->pager)) != CHIDB_OK ) {
        return rc;
    }
    free(bt);
    return CHIDB_OK;
}


/* Loads a B-Tree node from disk
 *
 * Reads a B-Tree node from a page in the disk. All the information regarding
 * the node is stored in a BTreeNode struct (see header file for more details
 * on this struct). *This is the only function that can allocate memory for
 * a BTreeNode struct*. Always use chidb_Btree_freeMemNode to free the memory
 * allocated for a BTreeNode (do not use free() directly on a BTreeNode variable)
 * Any changes made to a BTreeNode variable will not be effective in the database
 * until chidb_Btree_writeNode is called on that BTreeNode.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Page of node to load
 * - btn: Out parameter. Used to return a pointer to newly creater BTreeNode
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EPAGENO: The provided page number is not valid
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_getNodeByPage(BTree *bt, npage_t npage, BTreeNode **btn)
{
    // TODO: Fix memory leaks, if we fail to read page after mallocing, for instance.
    /* Your code goes here */
    int rc;

    *btn = malloc(sizeof(BTreeNode));
    if (btn == NULL) {
        return CHIDB_ENOMEM;
    }

    MemPage **page = &((*btn)->page);
    rc = chidb_Pager_readPage(bt->pager, npage, page);
    if (rc != CHIDB_OK) {
        chilog(ERROR, "couldn't read page: err %d, page %d", rc, npage);
        return rc;
    }

    // Parse the node from the page in memory
    // Special case first page, which contains the 100 byte db file header
    uint8_t *const data = (*page)->data + (npage == 1 ? 100 : 0);
    BTreeNode* node = *btn;
    node->type = data[0];
    node->free_offset = get2byte(data+1);
    node->n_cells = get2byte(data+3);
    node->cells_offset = get2byte(data+5);


    int is_internal_node = node->type == PGTYPE_TABLE_INTERNAL || node->type == PGTYPE_INDEX_INTERNAL;
    if (is_internal_node) {
        node->right_page = get4byte(data+8);
    }

    node->celloffset_array = data + (is_internal_node ? 12 : 8);

    return CHIDB_OK;
}


/* Frees the memory allocated to an in-memory B-Tree node
 *
 * Frees the memory allocated to an in-memory B-Tree node, and
 * the in-memory page returned by the pages (stored in the
 * "page" field of BTreeNode)
 *
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to free
 *
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_Btree_freeMemNode(BTree *bt, BTreeNode *btn)
{
    /* Your code goes here */
    int rc;
    rc = chidb_Pager_releaseMemPage(bt->pager, btn->page);
    if (rc != CHIDB_OK) {
        return rc;
    }

    free(btn);
    return CHIDB_OK;
}


/* Create a new B-Tree node
 *
 * Allocates a new page in the file and initializes it as a B-Tree node.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Out parameter. Returns the number of the page that
 *          was allocated.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *         PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_newNode(BTree *bt, npage_t *npage, uint8_t type)
{
    /* Your code goes here */
    int rc;
    // This function never fails lol
    chidb_Pager_allocatePage(bt->pager, npage);

    rc = chidb_Btree_initEmptyNode(bt, *npage, type);
    if (rc != CHIDB_OK) {
        return rc;
    }

    return CHIDB_OK;
}


/* Initialize a B-Tree node
 *
 * Initializes a database page to contain an empty B-Tree node. The
 * database page is assumed to exist and to have been already allocated
 * by the pager.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Database page where the node will be created.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *         PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_initEmptyNode(BTree *bt, npage_t npage, uint8_t type)
{
    /* Your code goes here */
    MemPage* page;
    int rc = chidb_Pager_readPage(bt->pager, npage, &page);
    if (rc != CHIDB_OK) {
        // could only be CHIDB_ENOMEM here
        return rc;
    }

    int is_internal_node = type == PGTYPE_TABLE_INTERNAL || type == PGTYPE_INDEX_INTERNAL;
    BTreeNode btn;
    btn.page = page;
    btn.type = type;
    // free offset starts right after the header, since there are
    // no cells yet
    btn.free_offset = (is_internal_node ? 12 : 8) + (npage == 1 ? 100 : 0);
    btn.n_cells = 0;
    // cells_offset points to the region where cells are stored,
    // which grows up from the bottom of the page. When we have
    // no cells, it should be set to page size.
    btn.cells_offset = bt->pager->page_size;
    if (is_internal_node) {
        btn.right_page = 0;
    }
    btn.celloffset_array = page->data + (is_internal_node ? 12 : 8);

    // write it back to disk
    rc = chidb_Btree_writeNode(bt, &btn);
    if (rc != CHIDB_OK) {
        return rc;
    }

    return CHIDB_OK;
}



/* Write an in-memory B-Tree node to disk
 *
 * Writes an in-memory B-Tree node to disk. To do this, we need to update
 * the in-memory page according to the chidb page format. Since the cell
 * offset array and the cells themselves are modified directly on the
 * page, the only thing to do is to store the values of "type",
 * "free_offset", "n_cells", "cells_offset" and "right_page" in the
 * in-memory page.
 *
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to write to disk
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_writeNode(BTree *bt, BTreeNode *btn)
{
    MemPage* page = btn->page;
    uint8_t* data = page->data;

    // First page is special because it holds file header
    if (page->npage == 1) {
        data += 100;
    }

    int is_internal_node = btn->type == PGTYPE_TABLE_INTERNAL || btn->type == PGTYPE_INDEX_INTERNAL;
    data[0] = btn->type;
    put2byte(&data[1], btn->free_offset);
    put2byte(&data[3], btn->n_cells);
    put2byte(&data[5], btn->cells_offset);
    if (is_internal_node) {
        put4byte(&data[8], btn->right_page);
    }

    int rc = chidb_Pager_writePage(bt->pager, page);
    if (rc != CHIDB_OK) {
        return rc;
    }

    return CHIDB_OK;
}


/* Read the contents of a cell
 *
 * Reads the contents of a cell from a BTreeNode and stores them in a BTreeCell.
 * This involves the following:
 *  1. Find out the offset of the requested cell.
 *  2. Read the cell from the in-memory page, and parse its
 *     contents (refer to The chidb File Format document for
 *     the format of cells).
 *
 * Parameters
 * - btn: BTreeNode where cell is contained
 * - ncell: Cell number
 * - cell: BTreeCell where contents must be stored.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_getCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{
    if (ncell >= btn->n_cells) {
        return CHIDB_ECELLNO;
    }
    // celloffset_array is u8, but offsets are two bytes.
    // so we need 2*ncell to get the index of the two bytes, and then
    // convert from big-endian to little-endian.
    const uint16_t celloffset = get2byte(&(btn->celloffset_array[2*ncell]));
    /*const*/ uint8_t* cell_data = btn->page->data + celloffset;

    cell->type = btn->type;
    switch (btn->type) {
        case PGTYPE_TABLE_INTERNAL:
            cell->fields.tableInternal.child_page = get4byte(&cell_data[0]);
            getVarint32(&cell_data[4], &cell->key);
            break;
        case PGTYPE_TABLE_LEAF:
            getVarint32(&cell_data[0], &cell->fields.tableLeaf.data_size);
            getVarint32(&cell_data[4], &cell->key);
            cell->fields.tableLeaf.data = &cell_data[8];
            break;
        case PGTYPE_INDEX_INTERNAL:
            cell->fields.indexInternal.child_page = get4byte(&cell_data[0]);
            cell->key = get4byte(&cell_data[8]);
            cell->fields.indexInternal.keyPk = get4byte(&cell_data[12]);
            break;
        case PGTYPE_INDEX_LEAF:
            cell->key = get4byte(&cell_data[4]);
            cell->fields.indexLeaf.keyPk = get4byte(&cell_data[8]);
            break;
        default:
            break;
    }

    return CHIDB_OK;
}


/* Insert a new cell into a B-Tree node
 *
 * Inserts a new cell into a B-Tree node at a specified position ncell.
 * This involves the following:
 *  1. Add the cell at the top of the cell area. This involves "translating"
 *     the BTreeCell into the chidb format (refer to The chidb File Format
 *     document for the format of cells).
 *  2. Modify cells_offset in BTreeNode to reflect the growth in the cell area.
 *  3. Modify the cell offset array so that all values in positions >= ncell
 *     are shifted one position forward in the array. Then, set the value of
 *     position ncell to be the offset of the newly added cell.
 *
 * This function assumes that there is enough space for this cell in this node.
 *
 * Parameters
 * - btn: BTreeNode to insert cell in
 * - ncell: Cell number
 * - cell: BTreeCell to insert.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_insertCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{
    if (ncell > btn->n_cells) {
        return CHIDB_ECELLNO;
    }
    // compute cell size in bytes
    unsigned int cell_size = 0;
    switch (btn->type) {
        case PGTYPE_TABLE_INTERNAL:
            cell_size = 8;
            break;
        case PGTYPE_TABLE_LEAF:
            // TODO: possible integer overflow here if data_size = 2**32-1
            cell_size = 8 + cell->fields.tableLeaf.data_size;
            break;
        case PGTYPE_INDEX_INTERNAL:
            cell_size = 16;
            break;
        case PGTYPE_INDEX_LEAF:
            cell_size = 12;
            break;
        default:
            break;
    }

    // subtract from cells_offset
    // todo: check that cells_offset > btn->page->data
    btn->cells_offset -= cell_size;
    /*const*/ uint8_t* cell_data = btn->page->data + btn->cells_offset;

    // write cell data
    switch (btn->type) {
        case PGTYPE_TABLE_INTERNAL:
            put4byte(cell_data, cell->fields.tableInternal.child_page);
            putVarint32(cell_data+4, cell->key);
            break;
        case PGTYPE_TABLE_LEAF:
            putVarint32(cell_data, cell->fields.tableLeaf.data_size);
            putVarint32(cell_data+4, cell->key);
            memcpy(cell_data+8,
                   cell->fields.tableLeaf.data,
                   cell->fields.tableLeaf.data_size);
            break;
        case PGTYPE_INDEX_INTERNAL:
            put4byte(cell_data, cell->fields.indexInternal.child_page);
            put4byte(cell_data+4, 0x0B030404); // idk what this magic num is
            put4byte(cell_data+8, cell->key);
            put4byte(cell_data+12, cell->fields.indexInternal.keyPk);
            break;
        case PGTYPE_INDEX_LEAF:
            put4byte(cell_data, 0x0B030404); // idk what this magic num is
            put4byte(cell_data+4, cell->key);
            put4byte(cell_data+8, cell->fields.indexLeaf.keyPk);
            break;
        default:
            break;
    }

    // update cell offset array; current value of cells_offset will be the new
    // value to write. cell offset array ends one byte before free_offset.
    const unsigned int cell_i = 2*ncell;
    // if we're inserting not at the end of the list, but inside, we need to shift
    // existing cell offsets over by two bytes.
    if (ncell < btn->n_cells) {
        const unsigned int n_cells_to_move = btn->n_cells - ncell;
        memmove(btn->celloffset_array + cell_i + 2,
                btn->celloffset_array + cell_i,
                2*n_cells_to_move);
    }
    put2byte(btn->celloffset_array + cell_i, btn->cells_offset);

    // Update free_offset
    // TODO: This is only correct if we can't add cells beyond the end of the array,
    // e.g., 0 <= ncell <= n_cells
    btn->free_offset += 2;

    // Update number of cells (we just added one)
    btn->n_cells++;
    return CHIDB_OK;
}

/* Find an entry in a table B-Tree
 *
 * Finds the data associated for a given key in a table B-Tree
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want search in
 * - key: Entry key
 * - data: Out-parameter where a copy of the data must be stored
 * - size: Out-parameter where the number of bytes of data must be stored
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: No entry with the given key way found
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_find(BTree *bt, npage_t nroot, chidb_key_t key, uint8_t **data, uint16_t *size)
{
    int ret = CHIDB_OK;
    BTreeNode* btn = NULL;
    int rc;
    npage_t npage = nroot;
    BTreeCell cell;

    // traverse until we reach a leaf node
    while (1) {
        if (btn != NULL) {
            chidb_Btree_freeMemNode(bt, btn);
        }
        rc = chidb_Btree_getNodeByPage(bt, npage, &btn);
        if (rc != CHIDB_OK) {
            chilog(ERROR, "Could not read page %d in Btree_find", npage);
            ret = rc;
            goto done;
        }
        if (btn->type == PGTYPE_INDEX_LEAF || btn->type == PGTYPE_TABLE_LEAF) {
            break;
        }
        ncell_t ncell;
        for (ncell = 0; ncell < btn->n_cells; ncell++) {
            rc = chidb_Btree_getCell(btn, ncell, &cell);
            if (rc != CHIDB_OK) {
                // panic, malformed or malicious db file
                // if 0 <= ncell < btn->n_cells, we should always have those cells available
                ret = rc;
                goto done;
            }
            if (key <= cell.key) {
                break;
            }
        }

        // If our key is greater than all cell keys, use right_page
        if (ncell == btn->n_cells) {
            npage = btn->right_page;
        }
            // If we're on an index internal node and have an exact key match,
            // return the corresponding primary key.
        else if (btn->type == PGTYPE_INDEX_INTERNAL && key == cell.key) {
            *size = (uint16_t)sizeof(chidb_key_t);
            *data = malloc(*size);
            if (*data == NULL) {
                ret = CHIDB_ENOMEM;
                goto done;
            }
            chidb_key_t* key_data = (chidb_key_t*)(*data);
            *key_data = cell.fields.indexInternal.keyPk;
            goto done;
        }
            // otherwise, we're on an internal node and need to follow a child pointer
        else {
            switch (cell.type) {
                case PGTYPE_INDEX_INTERNAL:
                    npage = cell.fields.indexInternal.child_page;
                    break;
                case PGTYPE_TABLE_INTERNAL:
                    npage = cell.fields.tableInternal.child_page;
                    break;
                default:
                    break;
            }
        }
    }

    // Now we know that btn is a leaf node.
    ncell_t ncell;
    for (ncell = 0; ncell < btn->n_cells; ncell++) {
        rc = chidb_Btree_getCell(btn, ncell, &cell);
        if (rc != CHIDB_OK) {
            // panic, malformed or malicious db file
            ret = rc;
            goto done;
        }

        // We're in a leaf node, so if key < cell.key, it's not in the btree at all.
        if (key < cell.key) {
            ret = CHIDB_ENOTFOUND;
            goto done;
        }
        if (key == cell.key) {
            break;
        }
    }

    // If our key is greater than all cells in the leaf, no match.
    if (ncell == btn->n_cells) {
        ret = CHIDB_ENOTFOUND;
        goto done;
    } else {
        switch(btn->type) {
            case PGTYPE_INDEX_LEAF:
                *size = (uint16_t)sizeof(chidb_key_t);
                *data = malloc(*size);
                if (*data == NULL) {
                    ret = CHIDB_ENOMEM;
                    goto done;
                }
                chidb_key_t* key_data = (chidb_key_t*)(*data);
                *key_data = cell.fields.indexLeaf.keyPk;
                goto done;
                break;
            case PGTYPE_TABLE_LEAF:
                *size = cell.fields.tableLeaf.data_size;
                *data = malloc(*size);
                if (*data == NULL) {
                    ret = CHIDB_ENOMEM;
                    goto done;
                }
                memcpy(*data, cell.fields.tableLeaf.data, *size);
                goto done;
                break;
            default:
                break;
        }
    }

    // After looping and copying data, we've still got a node in memory
    // so we free it
    done:
    chidb_Btree_freeMemNode(bt, btn);
    return ret;
}



/* Insert an entry into a table B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a key and data, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this entry in.
 * - key: Entry key
 * - data: Pointer to data we want to insert
 * - size: Number of bytes of data
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInTable(BTree *bt, npage_t nroot, chidb_key_t key, uint8_t *data, uint16_t size)
{
    BTreeCell cell;
    cell.type = PGTYPE_TABLE_LEAF;
    cell.key = key;
    cell.fields.tableLeaf.data_size = size;
    cell.fields.tableLeaf.data = data;

    int ret = chidb_Btree_insert(bt, nroot, &cell);
    return ret;
}


/* Insert an entry into an index B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a KeyIdx and a KeyPk, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this entry in.
 * - keyIdx: See The chidb File Format.
 * - keyPk: See The chidb File Format.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInIndex(BTree *bt, npage_t nroot, chidb_key_t keyIdx, chidb_key_t keyPk)
{
    BTreeCell cell;
    cell.type = PGTYPE_INDEX_LEAF;
    cell.key = keyIdx;
    cell.fields.indexLeaf.keyPk = keyPk;

    int ret = chidb_Btree_insert(bt, nroot, &cell);
    return ret;
}

bool chidb_Btree_nodeIsFull(BTreeNode *btn, BTreeCell *cell) {
    // compute cell size in bytes
    unsigned int cell_size = 0;
    switch (btn->type) {
        case PGTYPE_TABLE_INTERNAL:
            cell_size = 8;
            break;
        case PGTYPE_TABLE_LEAF:
            // TODO: possible integer overflow here if data_size = 2**32-1
            cell_size = 8 + cell->fields.tableLeaf.data_size;
            break;
        case PGTYPE_INDEX_INTERNAL:
            cell_size = 16;
            break;
        case PGTYPE_INDEX_LEAF:
            cell_size = 12;
            break;
        default:
            break;
    }

    unsigned int cell_offset_size = 2;
    uint16_t free_space = btn->cells_offset - btn->free_offset;

    return (cell_offset_size + cell_size) > free_space;
}

/* Insert a BTreeCell into a B-Tree
 *
 * The chidb_Btree_insert and chidb_Btree_insertNonFull functions
 * are responsible for inserting new entries into a B-Tree, although
 * chidb_Btree_insertNonFull is the one that actually does the
 * insertion. chidb_Btree_insert, however, first checks if the root
 * has to be split (a splitting operation that is different from
 * splitting any other node). If so, chidb_Btree_split is called
 * before calling chidb_Btree_insertNonFull.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insert(BTree *bt, npage_t nroot, BTreeCell *btc)
{
    BTreeNode* root;
    int rc;
    rc = chidb_Btree_getNodeByPage(bt, nroot, &root);
    if (rc != CHIDB_OK) {
        chilog(ERROR, "Could not read page %d in Btree_insert", nroot);
        return rc;
    }
    bool root_is_full = chidb_Btree_nodeIsFull(root, btc);
    rc = chidb_Btree_freeMemNode(bt, root);
    if (rc != CHIDB_OK) {
        return rc;
    }
    if (root_is_full) {
        npage_t npage_child2;
        rc = chidb_Btree_split(bt, 0, nroot, 0, &npage_child2);
        if (rc != CHIDB_OK) {
            return rc;
        }
    }

    return chidb_Btree_insertNonFull(bt, nroot, btc);
}

/* Insert a BTreeCell into a non-full B-Tree node
 *
 * chidb_Btree_insertNonFull inserts a BTreeCell into a node that is
 * assumed not to be full (i.e., does not require splitting). If the
 * node is a leaf node, the cell is directly added in the appropriate
 * position according to its key. If the node is an internal node, the
 * function will determine what child node it must insert it in, and
 * calls itself recursively on that child node. However, before doing so
 * it will check if the child node is full or not. If it is, then it will
 * have to be split first.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertNonFull(BTree *bt, npage_t npage, BTreeCell *btc)
{
    // If leaf, assume nonfull and insert.
    // If internal, find appropriate child and recurse.
    // TODO: check if child is full before recursing, if so, split child.
    int rc;
    int ret = -1;

    BTreeNode* btn;
    rc = chidb_Btree_getNodeByPage(bt, npage, &btn);
    if (rc != CHIDB_OK) {
        chilog(ERROR, "Couldn't get page: npage=%d, rc=%d", npage, rc);
        return rc;
    }

    // find cell where we should either insert or recurse
    // We insert if leaf, else we recurse.
    ncell_t ncell;
    BTreeCell cell;
    for (ncell = 0; ncell < btn->n_cells; ncell++) {
        rc = chidb_Btree_getCell(btn, ncell, &cell);
        if (rc != CHIDB_OK) {
            // panic, malformed or malicious db file
            chilog(ERROR, "Couldn't get cell: npage=%d, ncell=%d, rc=%d", npage, ncell, rc);
            ret = rc;
            goto done;
        }
        // if we've found a cell with a smaller or same key, then:
        //      if we're a leaf, insert here
        //      if we're internal, recurse here
        if (btc->key <= cell.key) {
            if (btc->key == cell.key && (cell.type == PGTYPE_INDEX_LEAF || cell.type == PGTYPE_TABLE_LEAF)) {
                rc = CHIDB_EDUPLICATE;
                goto done;
            }
            break;
        }
    }

    npage_t child_page;
    switch (btn->type) {
        case PGTYPE_INDEX_LEAF:
        case PGTYPE_TABLE_LEAF:
            ret = chidb_Btree_insertCell(btn, ncell, btc);
            if (ret != CHIDB_OK) {
                goto done;
            }
            ret = chidb_Btree_writeNode(bt, btn);
            break;
        case PGTYPE_INDEX_INTERNAL:
        case PGTYPE_TABLE_INTERNAL:
            if (ncell == btn->n_cells) {
                child_page = btn->right_page;
            } else {
                switch (cell.type) {
                    case PGTYPE_INDEX_INTERNAL:
                        child_page = cell.fields.indexInternal.child_page;
                        break;
                    case PGTYPE_TABLE_INTERNAL:
                        child_page = cell.fields.tableInternal.child_page;
                        break;
                    default:
                        chilog(ERROR, "should never happen");
                        break;
                }
            }
            // Check if child is full. If so, split it and then recurse.
            BTreeNode* child;
            ret = chidb_Btree_getNodeByPage(bt, child_page, &child);
            if (ret != CHIDB_OK) {
                chilog(ERROR, "Couldn't get page %d in Btree_insertNonFull", child_page);
                goto done;
            }
            bool child_is_full = chidb_Btree_nodeIsFull(child, btc);
            ret = chidb_Btree_freeMemNode(bt, child);
            if (ret != CHIDB_OK) {
                goto done;
            }

            npage_t npage_child2;
            if (child_is_full) {
                ret = chidb_Btree_split(bt, npage, child_page, ncell, &npage_child2);
                if (ret != CHIDB_OK) {
                    goto done;
                }
                // Redo the same page now that the child's been split.
                ret = chidb_Btree_insertNonFull(bt, npage, btc);
            } else {
                ret = chidb_Btree_insertNonFull(bt, child_page, btc);
            }
            goto done;
            break;
        default:
            break;
    }

    done:
    rc = chidb_Btree_freeMemNode(bt, btn);
    if (rc != CHIDB_OK) {
        // possible memory leak here
        return rc;
    }
    return ret;
}


/* Split a B-Tree node
 *
 * Splits a B-Tree node N. This involves the following:
 * - Find the median cell in N.
 * - Create a new B-Tree node M.
 * - Move the cells before the median cell to M (if the
 *   cell is a table leaf cell, the median cell is moved too)
 * - Add a cell to the parent (which, by definition, will be an
 *   internal page) with the median key and the page number of M.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage_parent: Page number of the parent node
 * - npage_child: Page number of the node to split
 * - parent_ncell: Position in the parent where the new cell will
 *                 be inserted.
 * - npage_child2: Out parameter. Used to return the page of the new child node.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_split(BTree *bt, npage_t npage_parent, npage_t npage_child, ncell_t parent_ncell, npage_t *npage_child2)
{
    int rc;
    int ret = CHIDB_OK;
    BTreeNode *parent;
    BTreeNode *child;
    BTreeNode *child1;
    BTreeNode *child2;
    BTreeCell median;
    bool is_root = npage_parent == 0;
    npage_t npage_child1;


    rc = chidb_Btree_getNodeByPage(bt, npage_child, &child);
    if (rc != CHIDB_OK) {
        return rc;
    }

    ret = chidb_Btree_newNode(bt, npage_child2, child->type);
    if (ret != CHIDB_OK) {
        goto err1;
    }
    ret = chidb_Btree_getNodeByPage(bt, *npage_child2, &child2);
    if (ret != CHIDB_OK) {
        goto err1;
    }

    // move everything up to and perhaps including the median into the new child node
    bool child_is_table_leaf = child->type == PGTYPE_TABLE_LEAF;
    ncell_t median_ncell = (child->n_cells)/2;
    BTreeCell cell;
    ncell_t ncell;
    for (ncell = 0;
         ncell <= (child_is_table_leaf ? median_ncell : median_ncell - 1);
         ncell++)
    {
        // TODO: check here if we passed an invalid cell number
        chidb_Btree_getCell(child, ncell, &cell);
        chidb_Btree_insertCell(child2, ncell, &cell);
    }

    chidb_Btree_getCell(child, median_ncell, &median);
    if (child2->type == PGTYPE_INDEX_INTERNAL) {
        child2->right_page = median.fields.indexInternal.child_page;
    } else if (child2->type == PGTYPE_TABLE_INTERNAL) {
        child2->right_page = median.fields.tableInternal.child_page;
    }

    // if this is a root node...
    if (is_root) {
        // Allocate a new page for the right half, child1, instead of re-using the same page.
        ret = chidb_Btree_newNode(bt, &npage_child1, child->type);
        if (ret != CHIDB_OK) {
            goto err2;
        }
    } else {
        // Wipe the page containing the node being split, so we can insert just elements after the median.
        // An old copy of the node is still in memory, since we don't use a page cache.
        // Since the child is not the root, the parent is still pointing to npage_child, so we reuse that page.
        ret = chidb_Btree_initEmptyNode(bt, npage_child, child->type);
        if (ret != CHIDB_OK) {
            goto err2;
        }
        npage_child1 = npage_child;
    }
    ret = chidb_Btree_getNodeByPage(bt, npage_child1, &child1);
    if (ret != CHIDB_OK) {
        goto err2;
    }
    // "move" everything after the median into child1;
    int j = 0;
    for (ncell = median_ncell+1; ncell < child->n_cells; ncell++, j++)
    {
        chidb_Btree_getCell(child, ncell, &cell);
        chidb_Btree_insertCell(child1, j, &cell);
    }
    if (child1->type == PGTYPE_INDEX_INTERNAL || child1->type == PGTYPE_TABLE_INTERNAL) {
        child1->right_page = child->right_page;
    }

    if (is_root) {
        // If we're splitting a root node, we need to reuse this page as the new parent.
        uint8_t type;
        if (child->type == PGTYPE_INDEX_INTERNAL || child->type == PGTYPE_INDEX_LEAF) {
            type = PGTYPE_INDEX_INTERNAL;
        } else {
            type = PGTYPE_TABLE_INTERNAL;
        }
        ret = chidb_Btree_initEmptyNode(bt, npage_child, type);
        if (ret != CHIDB_OK) {
            goto err3;
        }
        ret = chidb_Btree_getNodeByPage(bt, npage_child, &parent);
        if (ret != CHIDB_OK) {
            goto err3;
        }
    } else {
        ret = chidb_Btree_getNodeByPage(bt, npage_parent, &parent);
        if (ret != CHIDB_OK) {
            goto err3;
        }
    }

    // Write a cell containing the median key to the parent, which has already been split or is newly made, so definitely nonfull
    // todo: change median to be an internal cell if it's a leaf cell currently
    BTreeCell parent_median;
    parent_median.key = median.key;
    switch (median.type) {
        case PGTYPE_INDEX_INTERNAL:
            parent_median = median;
            parent_median.fields.indexInternal.child_page = *npage_child2;
            break;
        case PGTYPE_TABLE_INTERNAL:
            parent_median = median;
            parent_median.fields.tableInternal.child_page = *npage_child2;
            break;
        case PGTYPE_INDEX_LEAF:
            parent_median.type = PGTYPE_INDEX_INTERNAL;
            parent_median.fields.indexInternal.child_page = *npage_child2;
            parent_median.fields.indexInternal.keyPk = median.fields.indexLeaf.keyPk;
            break;
        case PGTYPE_TABLE_LEAF:
            parent_median.type = PGTYPE_TABLE_INTERNAL;
            parent_median.fields.tableInternal.child_page = *npage_child2;
            break;
        default:
            break;
    }
    chidb_Btree_insertCell(parent, parent_ncell, &parent_median);
    if (is_root) {
        parent->right_page = npage_child1;
    }

    // Write nodes back to pages
    ret = chidb_Btree_writeNode(bt, parent);
    if (ret != CHIDB_OK) {
        goto err4;
    }
    ret = chidb_Btree_writeNode(bt, child1);
    if (ret != CHIDB_OK) {
        goto err4;
    }
    ret = chidb_Btree_writeNode(bt, child2);
    if (ret != CHIDB_OK) {
        goto err4;
    }

    err4:
    rc = chidb_Btree_freeMemNode(bt, parent);
    if (rc != CHIDB_OK) {
        return rc;
    }
    err3:
    rc = chidb_Btree_freeMemNode(bt, child1);
    if (rc != CHIDB_OK) {
        return rc;
    }
    err2:
    rc = chidb_Btree_freeMemNode(bt, child2);
    if (rc != CHIDB_OK) {
        return rc;
    }
    err1:
    rc = chidb_Btree_freeMemNode(bt, child);
    if (rc != CHIDB_OK) {
        return rc;
    }
    return ret;
}

