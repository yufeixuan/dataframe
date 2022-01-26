package io.github.yufeixuan.impl;

import java.util.*;

/**
 * @Author: Luoxuan
 * @Date: 2021/12/23 12:19
 */
public class BlockManager<V> {
    private final List<List<V>> blocks;

    public BlockManager() {
        this(Collections.<List<V>>emptyList());
    }

    public BlockManager(final Collection<? extends Collection<? extends V>> data) {
        blocks = new LinkedList<>();
        for (final Collection<? extends V> col : data) {
            add(new ArrayList<>(col));
        }
    }

    public void reshape(final int cols, final int rows) {
        for (int c = blocks.size(); c < cols; c++) {
            add(new ArrayList<V>(rows));
        }

        // 给每列添加null到rows行的数量
        for (final List<V> block : blocks) {
            for (int r = block.size(); r < rows; r++) {
                block.add(null);
            }
        }
    }

    public V get(final int col, final int row) {
        return blocks.get(col).get(row);
    }

    public void set(final V value, final int col, final int row) {
        blocks.get(col).set(row, value);
    }

    public List<V> row(final int row) {
        List<V> rows = new LinkedList<>();
        for (int i = 0; i < blocks.size(); i++) {
            V col_data = blocks.get(i).get(row);
            rows.add(col_data);
        }
        return rows;
    }

    public void del(final int row) {
        for (int i = 0; i < blocks.size(); i++) {
            blocks.get(i).remove(row);
        }
    }


    public void add(final List<V> col) {
        final int len = length();
        for (int r = col.size(); r < len; r++) {
            col.add(null);
        }
        blocks.add(col);
    }

    public int size() {
        return blocks.size();
    }

    public int length() {
        return blocks.isEmpty() ? 0 : blocks.get(0).size();
    }

    public List<V> column(Integer col) {
        if (col >= blocks.size()) {
            return null;
        }
        return blocks.get(col);
    }

    public void drop(int colIndex) {
        if (colIndex <= blocks.size()) {
            blocks.remove(colIndex);
        }
    }

    public List<List<V>> getBlocks() {
        return blocks;
    }

    public void setBlocks(ArrayList blocksArray){
        if (blocksArray != null && blocksArray.size() > 0) {
            for (int i = 0; i < blocksArray.size(); i++) {
                blocks.add((List<V>) blocksArray.get(i));
            }
        }
    }
}
