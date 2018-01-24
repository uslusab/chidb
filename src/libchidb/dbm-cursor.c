/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine cursors
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


#include "dbm-cursor.h"

#include "chidb/log.h"

int chidb_dbm_cursor_init(chidb_dbm_cursor_t* cursor, chidb_dbm_cursor_type_t type, BTree* bt, npage_t nroot) {
    cursor->bt = bt;
    cursor->type = type;
    cursor->depth = 0;
    cursor->max_depth = DEFAULT_CURSOR_MAX_DEPTH;
    cursor->cells = malloc(DEFAULT_CURSOR_MAX_DEPTH * sizeof(ncell_t));
    if (cursor->cells == NULL) {
        return CHIDB_ENOMEM;
    }
    cursor->nodes = malloc(DEFAULT_CURSOR_MAX_DEPTH * sizeof(BTreeNode*));
    if (cursor->nodes == NULL) {
        return CHIDB_ENOMEM;
    }

    int rc = chidb_Btree_getNodeByPage(bt, nroot, &cursor->nodes[0]);
    return rc;
}

int chidb_dbm_cursor_free(chidb_dbm_cursor_t* cursor)
{
    if (cursor->type == CURSOR_UNSPECIFIED) {
        return CHIDB_OK;
    }
    for (int i = 0; i < cursor->max_depth; i++) {
        // TODO: lol what if bad page num?
        if (cursor->nodes[i] != NULL) {
            chidb_Btree_freeMemNode(cursor->bt, cursor->nodes[i]);
        }
    }
    free(cursor->nodes);
    free(cursor->cells);

    cursor->type = CURSOR_UNSPECIFIED;
    cursor->bt = NULL;
    cursor->nodes = NULL;
    cursor->cells = NULL;
    return CHIDB_OK;
}

ncell_t chidb_dbm_cursor_getCurrentCellNo(chidb_dbm_cursor_t* cursor) {
    return cursor->cells[cursor->depth];
}

BTreeNode* chidb_dbm_cursor_getCurrentNode(chidb_dbm_cursor_t* cursor) {
    return cursor->nodes[cursor->depth];
}

bool chidb_dbm_cursor_currentNodeIsLeaf(chidb_dbm_cursor_t* cursor) {
    BTreeNode* btn = chidb_dbm_cursor_getCurrentNode(cursor);
    return btn->type == PGTYPE_INDEX_LEAF || btn->type == PGTYPE_TABLE_LEAF;
}

int chidb_dbm_cursor_goDownCurrentCell(chidb_dbm_cursor_t* cursor)
{
    // This function assumes the cursor is currently pointing at an internal node.
    BTreeNode* btn = chidb_dbm_cursor_getCurrentNode(cursor);
    npage_t next_page;
    ncell_t cellno = chidb_dbm_cursor_getCurrentCellNo(cursor);

    if (cellno == btn->n_cells) {
        next_page = btn->right_page;
    } else {
        BTreeCell cell;
        chidb_Btree_getCell(btn, cellno, &cell);
        if (cell.type == PGTYPE_INDEX_INTERNAL) {
            next_page = cell.fields.indexInternal.child_page;
        } else {
            next_page = cell.fields.tableInternal.child_page;
        }
    }
    // Go to the next depth.
    cursor->depth++;
    // point to the first cell of the page
    cursor->cells[cursor->depth] = 0;
    int rc = chidb_Btree_getNodeByPage(cursor->bt, next_page, &cursor->nodes[cursor->depth]);
    if (rc != CHIDB_OK) {
        return rc;
    }
    return CHIDB_OK;
}

void chidb_dbm_cursor_goToParent(chidb_dbm_cursor_t* cursor)
{
    assert(cursor->depth > 0);
    chidb_Btree_freeMemNode(cursor->bt, chidb_dbm_cursor_getCurrentNode(cursor));
    cursor->nodes[cursor->depth] = NULL;
    cursor->depth--;
}

int chidb_dbm_cursor_next(chidb_dbm_cursor_t* cursor) {
    assert(chidb_dbm_cursor_getCurrentNode(cursor)->type != PGTYPE_TABLE_INTERNAL);
    int ret = CHIDB_OK;
    BTreeNode* btn = chidb_dbm_cursor_getCurrentNode(cursor);
    /*
    if (!chidb_dbm_cursor_currentNodeIsLeaf(cursor) && btn->type == PGTYPE_TABLE_INTERNAL) {
        chilog(ERROR, "Next on page=%d, cell=%d, ncells=%d, depth=%d, leaf=%d", btn->page->npage,
            chidb_dbm_cursor_getCurrentCellNo(cursor),
            btn->n_cells, cursor->depth, chidb_dbm_cursor_currentNodeIsLeaf(cursor));
    }
    */
    // If there's a next cell we can easily go to, just go to it.
    if (chidb_dbm_cursor_getCurrentCellNo(cursor) + 1 < btn->n_cells) {
        //chilog(ERROR, "Easy next cell");
        cursor->cells[cursor->depth]++;
        // If we're on an index internal node, then the last cell we were pointing to had the correct value,
        // but now we want to go down the left subtree of the next cell to get to the next index cell.
        if (btn->type == PGTYPE_INDEX_INTERNAL) {
            while (!chidb_dbm_cursor_currentNodeIsLeaf(cursor)) {
                //chilog(ERROR, "Going to next on internal leaf");
                ret = chidb_dbm_cursor_goDownCurrentCell(cursor);
                if (ret != CHIDB_OK) {
                    goto done;
                }
            }
        }
        goto done;
    }

        // If there's no more cells in this node, then...
        //      If the node is a leaf, go to the parent.
        //          If we had followed the right_page of the parent, i.e., current cellno in parent is n_cells, then
        //              go up the tree until we find a node that we haven't followed the right_page for. If we get
        //              all the way to the root, we have no next cell.
        //          If the parent is an index node, stop, and leave the cursor pointing at that cell.
        //          Else, go to the next cell in the parent, and then drop down into that cell's child.
        //              If there is no next cell, follow the right_page pointer. (increment cellno anyway)
        //              Repeat until we find a leaf.
        //      If the node is not a leaf, it must be an index internal. Then,
        //          Go down the right_page, then follow down to a leaf along the left.
        //          Repeat until we find a leaf.

        // if we were just pointing to n_cells-1, and we're a leaf, we have to go up.
    else if (chidb_dbm_cursor_currentNodeIsLeaf(cursor)) {
        //chilog(ERROR, "No easy next, and is leaf");
        // First check if we're at the last entry in the whole tree
        // We know that the cursor is currently pointing at the last cell in a leaf,
        // so if we followed only right pages to get here, it's the last cell in the whole tree
        // special case if the root is a leaf.
        if (cursor->depth == 0) {
            ret = CHIDB_CURSOR_ENONEXT;
            goto done;
        }
        bool all_right = true;
        for (int i = cursor->depth-1; i >= 0; i--) {
            if (cursor->cells[i] != cursor->nodes[i]->n_cells) {
                // Not a right page
                all_right = false;
                break;
            }
        }
        if (all_right) {
            // Don't move the cursor and just return an error
            ret = CHIDB_CURSOR_ENONEXT;
            goto done;
        }
        do {
            //chilog(ERROR, "Going up to closest ancestor we didn't follow a right page for");
            // either the root is a leaf, or we went up to the root and we've already been down its right page.
            if (cursor->depth == 0) {
                ret = CHIDB_CURSOR_ENONEXT;
                goto done;
            }

            // If we're not at the root, go up one level.
            chidb_dbm_cursor_goToParent(cursor);
            btn = chidb_dbm_cursor_getCurrentNode(cursor);
        } while (chidb_dbm_cursor_getCurrentCellNo(cursor) >= btn->n_cells);
        // Now we are at the closest ancestor that has a next cell, or a right page that hasn't been followed yet.
        // If it's an index node, then stop; the current cell contains
        // the smallest value greater than all values in the node we just came from.
        // it would have been set to this before we went down into its child.
        if (btn->type == PGTYPE_INDEX_INTERNAL) {
            goto done;
        }

        // Otherwise, go to the next cell / right page.
        // At this point, we're looking at a table internal node.
        cursor->cells[cursor->depth]++;
        while (!chidb_dbm_cursor_currentNodeIsLeaf(cursor)) {
            //chilog(ERROR, "Going down left path after following a right page");
            ret = chidb_dbm_cursor_goDownCurrentCell(cursor);
            if (ret != CHIDB_OK) {
                goto done;
            }
        }

    }
        // no next cell and not a leaf, so we must be on an index internal
    else if (btn->type == PGTYPE_INDEX_INTERNAL) {
        //chilog(ERROR, "index internal");
        // increment cell number at this depth to indicate that we followed right_page
        cursor->cells[cursor->depth] = btn->n_cells;
        while (!chidb_dbm_cursor_currentNodeIsLeaf(cursor)) {
            //chilog(ERROR, "Going down left path after following a right page");
            ret = chidb_dbm_cursor_goDownCurrentCell(cursor);
            if (ret != CHIDB_OK) {
                goto done;
            }
        }
    }
        // Really shouldn't get here, cursor should never be pointing at a table internal cell
    else {
        chilog(ERROR, "Cursor is pointing at a table internal cell! This should never happen!");
        goto done;
    }

    done:
    assert(chidb_dbm_cursor_getCurrentNode(cursor)->type != PGTYPE_TABLE_INTERNAL);
    return ret;
}

int chidb_dbm_cursor_prev(chidb_dbm_cursor_t* cursor) {
    assert(chidb_dbm_cursor_getCurrentNode(cursor)->type != PGTYPE_TABLE_INTERNAL);
    int ret = CHIDB_OK;
    BTreeNode* btn;

    if (chidb_dbm_cursor_getCurrentCellNo(cursor) - 1 >= 0) {
        cursor->cells[cursor->depth]--;
        // TODO index
        goto done;
    } else {
        if (cursor->depth == 0) {
            ret = CHIDB_CURSOR_ENOPREV;
            goto done;
        }
        bool all_left = true;
        for (int i = cursor->depth-1; i >= 0; i--) {
            if (cursor->cells[i] > 0) {
                all_left = false;
                break;
            }
        }
        // if we're in the left-most cell
        if (all_left) {
            ret = CHIDB_CURSOR_ENOPREV;
            goto done;
        }

        do {
            chidb_dbm_cursor_goToParent(cursor);
        } while (chidb_dbm_cursor_getCurrentCellNo(cursor) == 0);

        // now we're at the first ancestor that has a prev cell
        // TODO: handle index internal
        cursor->cells[cursor->depth]--;
        while (!chidb_dbm_cursor_currentNodeIsLeaf(cursor)) {
            ret = chidb_dbm_cursor_goDownCurrentCell(cursor);
            if (ret != CHIDB_OK) {
                goto done;
            }
            btn = chidb_dbm_cursor_getCurrentNode(cursor);
            cursor->cells[cursor->depth] = btn->n_cells;
        }
        cursor->cells[cursor->depth] = btn->n_cells-1;
    }

    done:
    return ret;
}


ncell_t findCell(BTreeNode* btn, chidb_key_t key, BTreeCell* cell) {
    // TODO: Make this a binary search
    ncell_t i;
    for (i = 0; i < btn->n_cells; i++) {
        chidb_Btree_getCell(btn, i, cell);
        if (key <= cell->key) {
            break;
        }
    }
    return i;
}

void seek_partial(chidb_dbm_cursor_t* cursor, chidb_key_t key, BTreeCell* cell, ncell_t* i)
{
    // Go up to root
    while (cursor->depth != 0) {
        chidb_dbm_cursor_goToParent(cursor);
    }
    BTreeNode* btn;

    while (!chidb_dbm_cursor_currentNodeIsLeaf(cursor)) {
        btn = chidb_dbm_cursor_getCurrentNode(cursor);
        *i = findCell(btn, key, cell);
        cursor->cells[cursor->depth] = *i;
        if (btn->type == PGTYPE_INDEX_INTERNAL && cell->key == key) {
            // If we found an exact match on an index internal node, then we're done
            return;
        }
        // todo: check return code here
        chidb_dbm_cursor_goDownCurrentCell(cursor);
    }
    // Now we're at a leaf node, go to the cell where the key will match or not.
    btn = chidb_dbm_cursor_getCurrentNode(cursor);
    *i = findCell(btn, key, cell);
    cursor->cells[cursor->depth] = *i;

    // if *i == n_cells, then we got to the end of a leaf where key > all cells.
    // for a table, this means we must have come from a right_page pointer. if it wasn't a right page, then
    // key must be <= the value of the parent cell, and in a table, that parent cell's value is also in
    // the last cell of the child node. we can't get into a child like that and also be > than the last cell.
    // so we must be in the right-most cell. this always means key not found.
    // if the btree is an index, then we might have found an internal node cell where key < cell.key,
    // and we wouldn't expect to see that value in a child page. i.e., key is in between the parent cell
    // and the last cell of the child node.

    // if table and key not in table, we'll end up pointing to the first cell where key < cell.key, or right
    // past the end of the leaf node all the way on the right, if the key is greater than everything in the table.
    // if index and key not in index, we'll end up pointing to the first cell where key < cell.key
    // or right past the end of some leaf node, could be any leaf node. if past the end, then the
    // first cell with key < cell.key is the parent cell we followed, or, if the parent was a right page,
    // the first ancestor that's not a right page. if it's right pages all the way up to the root,
    // then our key is > everything in the tree.

    // TODO: if we seeked all the way past the last entry in the tree, make sure the cursor is pointing
    // to the last entry before we return.
}
int chidb_dbm_cursor_seek(chidb_dbm_cursor_t* cursor, chidb_key_t key) {
    BTreeCell cell;
    BTreeNode* btn;
    ncell_t i;

    seek_partial(cursor, key, &cell, &i);
    //
    // what if i == ncells? then cell is the wrong cell, ncell = i-1
    // but also, it means key not found and we need to look at next cell
    btn = chidb_dbm_cursor_getCurrentNode(cursor);
    if (i == btn->n_cells) {
        return CHIDB_CURSOR_EKEYNOTFOUND;
    }
    // otherwise, we did find a cell, but key still might not be there.
    if (key != cell.key) {
        return CHIDB_CURSOR_EKEYNOTFOUND;
    }

    return CHIDB_OK;
}

int chidb_dbm_cursor_seekge(chidb_dbm_cursor_t* cursor, chidb_key_t key) {
    BTreeCell cell;
    BTreeNode* btn;
    ncell_t i;

    seek_partial(cursor, key, &cell, &i);
    btn = chidb_dbm_cursor_getCurrentNode(cursor);
    if (i == btn->n_cells) {
        // if we're a table, then key greater than all in the tree.
        if (btn->type == PGTYPE_TABLE_LEAF) {
            return CHIDB_CURSOR_EKEYNOTFOUND;
        }
            // if we're in an index, then we need to go to the next cell.
        else if (btn->type == PGTYPE_INDEX_INTERNAL || btn->type == PGTYPE_INDEX_LEAF) {
            int rc = chidb_dbm_cursor_next(cursor);
            if (rc == CHIDB_CURSOR_ENONEXT) {
                return CHIDB_CURSOR_EKEYNOTFOUND;
            } else {
                return CHIDB_OK;
            }
        }
    } else {
        // If we're a table, we should be at a leaf node, and key <= cell.key, so just return it.
        if (btn->type == PGTYPE_TABLE_LEAF) {
            return CHIDB_OK;
        }
            // If we're an index, we might be at a leaf node or an internal node.
            // we'll only be at an internal node if key == cell.key. If we're at a leaf node,
            // check if key == cell.key. if so, we're done. if not, go to next cell.
        else if (btn->type == PGTYPE_INDEX_INTERNAL) {
            // assert key == cell.key
            return CHIDB_OK;
        }
        else if (btn->type == PGTYPE_INDEX_LEAF) {
            if (key > cell.key) {
                int rc = chidb_dbm_cursor_next(cursor);
                if (rc == CHIDB_CURSOR_ENONEXT) {
                    return CHIDB_CURSOR_EKEYNOTFOUND;
                }
            }
            return CHIDB_OK;
        } else {
            chilog(ERROR, "Pointing to a table internal, should never happen");
        }
    }

    return CHIDB_OK;
}

int chidb_dbm_cursor_seekgt(chidb_dbm_cursor_t* cursor, chidb_key_t key)
{
    BTreeCell cell;
    BTreeNode* btn;
    ncell_t i;

    seek_partial(cursor, key, &cell, &i);
    btn = chidb_dbm_cursor_getCurrentNode(cursor);
    assert(btn->type != PGTYPE_TABLE_INTERNAL);
    if (i == btn->n_cells) {
        // If we're on INDEX_INTERNAL, then we better have found an actual match,
        // and i < n_cells
        assert(btn->type != PGTYPE_INDEX_INTERNAL);
        // we've found no match if table, and need to go up to a parent if index, so
        // either way, try to go next. if index, it's guaranteed that the parent node
        // won't be ==, since otherwise we would have stopped when we hit it. if table,
        // go next will definitely return ENONEXT.
        int rc = chidb_dbm_cursor_next(cursor);
        if (rc == CHIDB_CURSOR_ENONEXT) {
            return CHIDB_CURSOR_EKEYNOTFOUND;
        }
        return CHIDB_OK;
    } else {
        if (key == cell.key) {
            int rc = chidb_dbm_cursor_next(cursor);
            if (rc == CHIDB_CURSOR_ENONEXT) {
                return CHIDB_CURSOR_EKEYNOTFOUND;
            }
        }
        // if key < cell.key, we're done.
        return CHIDB_OK;
    }

    return CHIDB_OK;
}

int chidb_dbm_cursor_rewind(chidb_dbm_cursor_t* cur)
{
    int rc;
    // Go to first entry in btree
    // First go up to root, then go down the left
    while (cur->depth != 0) {
        chidb_dbm_cursor_goToParent(cur);
    }
    // go to first cell
    cur->cells[cur->depth] = 0;
    // Go down the left to a leaf, this is the first node.
    while (!chidb_dbm_cursor_currentNodeIsLeaf(cur)) {
        rc = chidb_dbm_cursor_goDownCurrentCell(cur);
        if (rc != CHIDB_OK) {
            return rc;
        }
    }

    chilog(ERROR, "current node is leaf=%d, cellno=%d", chidb_dbm_cursor_currentNodeIsLeaf(cur), chidb_dbm_cursor_getCurrentCellNo(cur));

    return CHIDB_OK;
}