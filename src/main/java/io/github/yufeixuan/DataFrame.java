package io.github.yufeixuan;

import io.github.yufeixuan.impl.BlockManager;
import io.github.yufeixuan.impl.Combining;
import io.github.yufeixuan.impl.Sorting;

import java.util.*;

/**
 * @Author: Luoxuan
 * @Date: 2021/12/23 9:34
 */
public class DataFrame<V> implements Iterable<List<V>> {

    private Map<Object, Integer> index = new LinkedHashMap<>();
    private LinkedList<Object> columns = new LinkedList<>();
    private BlockManager<V> data = new BlockManager<>();


    /**
     * An enumeration of join types for joining data frames together.
     */
    public enum JoinType {
        INNER,
        OUTER,
        LEFT,
        RIGHT
    }

    public enum SortDirection {
        ASCENDING,
        DESCENDING
    }

    /**
     * 获取指定列对应的索引
     * @param name
     * @return
     */
    public Integer getColIndex(final Object name) {
        return this.index.get(name);
    }

    /**
     * 获取列数量
     * @return
     */
    public Integer getIndexSize() {
        return index.size();
    }

    private void addColIndex(final Object name, final Integer value) {
        if (index.put(name, value) != null) {
            throw new IllegalArgumentException("column name: '" + name +  "' is exist");
        }
    }

    /**
     * 重建index索引
     */
    private void reColIndex() {
        index.clear();
        for (int i = 0; i < columns.size(); i++) {
            addColIndex(columns.get(i), i);
        }
    }

    private void setColumns(LinkedList<Object> columns) {
        this.columns = columns;
        for (int i = 0; i < columns.size(); i++) {
            addColIndex(columns.get(i), i);
        }
    }

    /**
     * 获取所有列list
     * @return
     */
    public LinkedList<Object> getColumns() {
        return columns;
    }

    public DataFrame(final String ... columns) {
        LinkedList<Object> col = new LinkedList<>();
        for (String column: columns) {
            col.add(column);
        }
        setColumns(col);
    }

    public DataFrame(LinkedList<Object> columns) {
        setColumns(columns);
    }

    /**
     * 添加一行数据
     * @param row
     * @return
     */
    public DataFrame append(List<? extends V> row) {
        int len = data.length();
        data.reshape(columns.size(), len + 1);
        // 往最后一行增加遍历列数据
        for (int i = 0; i < columns.size(); i++) {
            data.set(row.get(i), i, len);
        }
        return this;
    }

    /**
     * 返回数据的长度
     * @return
     */
    public int length() {
        return data.length();
    }

    /**
     * 返回第row行的所有数据
     * @param row 行数，从0开始
     * @return
     */
    public List<V> row(final Integer row) {
        return data.row(row);
    }

    /**
     * 返回第col列的所有数据
     * @param col 列数，从0开始
     * @return
     */
    public List<V> column(final Integer col) {
        return data.column(col);
    }

    /**
     * 获取第row行第col列的数据
     * @param row 行数，从0开始
     * @param col 列数，从0开始
     * @return
     */
    public V get(int row, int col) {
        return data.get(col, row);
    }

    /**
     * 获取第row行列为col的数据
     * @param row 行数，从0开始
     * @param col 列名
     * @return
     */
    public V get(int row, String col) {
        return data.get(getColIndex(col), row);
    }

    /**
     * 设置值
     * @param row 行数
     * @param col 列数
     * @param val 值
     */
    public void set(int row, int col, V val) {
        data.set(val, col, row);
    }

    /**
     * 重命名列名
     * @param old 源列名
     * @param name 新列名
     * @return
     */
    public DataFrame<V> rename(final Object old, final Object name) {
        Integer colIndex = getColIndex(old);
        if (colIndex == null) {
            throw new IllegalArgumentException("列名不存在:" + old);
        }

        if (getColIndex(name) != null) {
            throw new IllegalArgumentException("新列名已经存在:" + name);
        }

        columns.set(colIndex, name);

        reColIndex();
        return this;
    }

    /**
     * 批量重命名列名
     * @param cols
     * @return
     */
    public DataFrame<V> rename(Map<Object, Object> cols) {
        for (Map.Entry<Object, Object> col : cols.entrySet() ) {
            rename(col.getKey(), col.getValue());
        }
        return this;
    }


    /**
     * 删除指定列
     * @param cols
     * @return
     */
    public DataFrame<V> drop(final Integer ... cols) {
        for (final int col : cols) {
            index.remove(columns.get(col));
            columns.remove(col);
            data.drop(col);
        }
        reColIndex();
        return this;
    }

    /**
     * 添加一列
     * @param col 列名
     * @return
     */
    public DataFrame<V> add(String col) {
        columns.add(col);
        index.put(col, columns.size() - 1);
        List<V> colArr = new LinkedList<>();
        data.add(colArr);
        return this;
    }

    /**
     * 填充map的key为列名，map的值作为填充的值
     * @param map
     * @return
     */
    public DataFrame<V> fillNaMap(Map<Object, Object> map) {
        for (Map.Entry<Object, Object> m : map.entrySet() ) {
            Integer colIndex = getColIndex(m.getKey());
            if (colIndex == null) {
                throw new IllegalArgumentException("列名不存在");
            }

            int len = length();

            for (int i = 0; i < len; i++) {
                if (get(i, colIndex) == null) {
                    set(i, colIndex, (V)m.getValue());
                }
            }
        }

        return this;
    }

    /**
     * 用户val填充col为null的列
     * @param col 列数
     * @param val 值
     * @return
     */
    public DataFrame<V> fillNa(String col, V val) {
        Integer colIndex = getColIndex(col);
        if (colIndex == null) {
            throw new IllegalArgumentException("列名不存在");
        }
        int len = length();

        for (int i = 0; i < len; i++) {
            if (get(i, colIndex) == null) {
                set(i, colIndex, val);
            }
        }

        return this;
    }

    /**
     * 复制源列的数据到目标列
     * @param sourceCol 源列
     * @param targetCol 目标列
     * @param isNa 源列是否为null
     * @return
     */
    public DataFrame<V> copy(String sourceCol, String targetCol, boolean isNa) {
        Integer sourceColIndex = getColIndex(sourceCol);
        Integer targetColIndex = getColIndex(targetCol);
        if (sourceColIndex == null || targetColIndex == null) {
            throw new IllegalArgumentException("源列名或目标列名表不存在");
        }

        int len = length();
        if (isNa) {
            for (int i = 0; i < len; i++) {
                if (get(i, targetColIndex) == null) {
                    set(i, targetColIndex, get(i, sourceColIndex));
                }
            }
        }
        else {
            for (int i = 0; i < len; i++) {
                set(i, targetColIndex, get(i, sourceColIndex));
            }
        }

        return this;
    }

    /**
     * 给指定列进行pow
     * @param num 底数
     * @param col 指数所在列名
     * @return
     */
    public DataFrame<V> pow(Double num, Object col) {
        Integer colIndex = getColIndex(col);
        if (colIndex == null) {
            throw new IllegalArgumentException("列名不存在");
        }
        int len = length();
        for (int i = 0; i < len; i++) {
            V colVal =  get(i, colIndex);
            Integer val = 1;
            if (colVal != null) {
                val = (int)Math.pow(num, Double.valueOf(String.valueOf(colVal)));
            }
            set(i, colIndex, (V) val);
        }
        return this;
    }

    public DataFrame<V> changeOnNaCondition(List colList, String col, int condition, double change) {
        Integer colIndex = getColIndex(col);
        if (colIndex == null) {
            throw new IllegalArgumentException("列名不存在");
        }
        int len = length();
        for (int i = 0; i < len; i++) {
            int isNullNum = 0;
            for (Object c : colList) {
                if (get(i, (String) c) == null) {
                    isNullNum++;
                }
            }

            if (isNullNum > condition) {
                String valStr = String.valueOf(get(i, colIndex));
                double dVal = Double.valueOf(valStr);
                Double changVal =  dVal + change;
                set(i, colIndex, (V) changVal);
            }
        }

        return this;
    }

    /**
     * 获取指定列最大值
     * @param col
     * @return
     */
    public V max(V col) {
        V max = getVal(col, -1);
        return max;
    }

    /**
     * 获取指定列最小值
     * @param col
     * @return
     */
    public V min(V col) {
        V min = getVal(col, 1);
        return min;
    }

    private V getVal(V col, final int sort) {
        Integer colIndex = getColIndex(col);
        if (colIndex == null) {
            throw new IllegalArgumentException("列名不存在");
        }
        List<V> columnList = column(colIndex);

        if (columnList == null) {
            return null;
        }

        Collections.sort(columnList, new Comparator<V> () {
            @Override
            public int compare(V o1, V o2) {
                int result = 0;
                Comparable v1 = Comparable.class.cast(o1);
                Comparable v2 = Comparable.class.cast(o2);
                result = v1.compareTo(v2) * sort;
                return result;
            }
        });

        return columnList.get(0);
    }

    /**
     * 删除指定列
     * @param cols
     * @return
     */
    public DataFrame<V> drop(final Object ... cols) {
        return drop(indices(cols));
    }

    /**
     * A function that is applied to objects (rows or values)
     * in a {@linkplain DataFrame data frame}.
     *
     * <p>Implementors define {@link #apply(Object)} to perform
     * the desired calculation and return the result.</p>
     *
     * @param <I> the type of the input values
     * @param <O> the type of the output values
     */
    public interface Function<I, O> {
        /**
         * Perform computation on the specified
         * input value and return the result.
         *
         * @param value the input value
         * @return the result
         */
        O apply(I value);
    }

    /**
     * A function that converts {@linkplain DataFrame data frame}
     * rows to index or group keys.
     *
     * <p>Implementors define {@link #apply(Object)} to accept
     * a data frame row as input and return a key value.</p>
     *
     * @param <I> the type of the input values
     */
    public interface KeyFunction<I> extends Function<List<I>, Object> { }


    /**
     * Return a new data frame created by performing a join of this
     * data frame with the argument using the specified join type and
     * the column values as the join key.
     *
     * @param other the other data frame
     * @param join the join type
     * @param cols the indices of the columns to use as the join key
     * @return the result of the join operation as a new data frame
     */
    public final DataFrame<V> joinOn(final DataFrame<V> other, final JoinType join, final Integer ... cols) {
        return Combining.joinOn(this, other, join, cols);
    }

    /**
     * Return a new data frame created by performing a join of this
     * data frame with the argument using the specified join type and
     * the column values as the join key.
     *
     * @param right the other data frame
     * @param join the join type
     * @param colKey the names of the columns to use as the join key
     * @return the result of the join operation as a new data frame
     */
    public final DataFrame<V> joinOn(final DataFrame<V> right, final JoinType join, final Object ... colKey) {
        return joinOn(right, join, indices(colKey));
    }


    /**
     * 排序
     * @param cols
     * @return
     */
    public DataFrame<V> sortBy(final Object ... cols) {
        final Map<Integer, SortDirection> sortCols = new LinkedHashMap<>();

        for (final Object col : cols) {
            final String str = col instanceof String ? String.class.cast(col) : "";
            final SortDirection dir = str.startsWith("-") ? SortDirection.DESCENDING : SortDirection.ASCENDING;
            final int c = this.getColIndex(str.startsWith("-") ? str.substring(1) : col);
            sortCols.put(c, dir);
        }

        return Sorting.sort(this, sortCols);
    }

    private Integer[] indices(final Object[] names) {
        return indices(Arrays.asList(names));
    }

    private Integer[] indices(final List<Object> names) {
        final int size = names.size();
        final Integer[] indices = new Integer[size];
        for (int i = 0; i < size; i++) {
            indices[i] = getColIndex(names.get(i));
        }
        return indices;
    }

    /**
     * 对指定列去重
     * @param cols 列名
     * @return
     */
    public DataFrame<V> unique(final Object ... cols) {
        return unique(indices(cols));
    }

    /**
     * 对指定列去重
     * @param cols 列索引
     * @return
     */
    public DataFrame<V> unique(final Integer ... cols) {
        final DataFrame<V> unique = new DataFrame<V>(getColumns());
        final Set<List<V>> seen = new HashSet<List<V>>();

        final int len = length();
        final List<V> key = new ArrayList<V>();

        for (final int c : cols) {
            for (int r = 0; r < len; r++) {
                key.add(get(r, c));
                if (!seen.contains(key)) {
                    unique.append(row(r));
                    seen.add(key);
                }
                key.clear();
            }

            seen.clear();
        }

        return unique;
    }

    @Override
    public Iterator<List<V>> iterator() {
        return new DataFrameIterator();
    }

    private class DataFrameIterator implements Iterator<List<V>> {
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index != data.length();
        }

        @Override
        public List<V> next() {
            return data.row(index++);
        }
    }
}
