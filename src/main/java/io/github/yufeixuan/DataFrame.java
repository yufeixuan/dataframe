package io.github.yufeixuan;

import io.github.yufeixuan.impl.BlockManager;
import io.github.yufeixuan.impl.Combining;
import io.github.yufeixuan.impl.Sorting;

import java.util.*;
import java.util.stream.Collectors;

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
     */
    public Integer getColIndex(final Object name) {
        return this.index.get(name);
    }

    /**
     * 获取列数量
     */
    public Integer getIndexSize() {
        return index.size();
    }

    public Map<Object, Integer> getIndex() {
        return index;
    }

    public List<List<V>> getBlocks() {
        return data.getBlocks();
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
     */
    public int length() {
        return data.length();
    }

    /**
     * 返回第row行的所有数据
     * @param row 行数，从0开始
     */
    public List<V> row(final Integer row) {
        return data.row(row);
    }

    /**
     * 返回第col列的所有数据
     * @param col 列数，从0开始
     */
    public List<V> column(final Integer col) {
        return data.column(col);
    }

    /**
     * 获取第row行第col列的数据
     * @param row 行数，从0开始
     * @param col 列数，从0开始
     */
    public V get(int row, int col) {
        return data.get(col, row);
    }

    /**
     * 获取第row行列为col的数据
     * @param row 行数，从0开始
     * @param col 列名
     */
    public V get(int row, String col) {
        Integer colIndex = getColIndex(col);
        if (colIndex == null) {
            throw new IllegalArgumentException("列名不存在:" + col);
        }
        return data.get(colIndex, row);
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
     * 设置指定行指定列的值
     * @param row 指定行索引
     * @param colName 指定列列名
     * @param val 设定值
     */
    public void set(int row, String colName, V val) {
        Integer colIndex = getColIndex(colName);
        if (colIndex == null) {
            throw new IllegalArgumentException("列名不存在:" + colName);
        }
        data.set(val, colIndex, row);
    }

    /**
     * 重命名列名
     * @param old 源列名
     * @param name 新列名
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
     * 删除指定行
     * @param rows 行索引
     */
    public DataFrame<V> dropRow(List<Integer> rows) {
        for (final int row : rows) {
            data.del(row);
        }
        return this;
    }


    /**
     * 添加一列
     * @param col 列名
     */
    public DataFrame<V> add(String col) {
        columns.add(col);
        index.put(col, columns.size() - 1);
        int len = data.length();

        for (int c = data.size(); c < columns.size(); c++) {
            data.add(new ArrayList<V>(len + 1));
        }

        return this;
    }

    @Override
    public DataFrame<V> clone() {
        LinkedList<Object> columns = getColumns();
        LinkedList<Object> newCols = new LinkedList<>();
        columns.forEach(col -> {
            newCols.add(col);
        });

        DataFrame<V> df = new DataFrame<>(newCols);
        for (int i = 0; i < length(); i++) {
            df.append(this.row(i));
        }

        return df;
    }

    /**
     * 填充map的key为列名，map的值作为填充的值
     * @param map
     */
    public DataFrame<V> fillNaMap(Map<Object, Object> map) {
        for (Map.Entry<Object, Object> m : map.entrySet() ) {
            Integer colIndex = getColIndex(m.getKey());
            if (colIndex == null) {
                throw new IllegalArgumentException("列名不存在" + m.getKey());
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
     */
    public DataFrame<V> fillNa(String col, V val) {
        Integer colIndex = getColIndex(col);
        if (colIndex == null) {
            throw new IllegalArgumentException("列名不存在" + col);
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
     * 给指定列进行pow,返回指定列的Double值
     * @param num 底数
     * @param col 指数所在列名
     */
    public DataFrame<V> pow(Double num, Object col) {
        Integer colIndex = getColIndex(col);
        if (colIndex == null) {
            throw new IllegalArgumentException("列名不存在" + col);
        }
        int len = length();
        for (int i = 0; i < len; i++) {
            V colVal =  get(i, colIndex);
            Double val = 1.00000000d;
            if (colVal != null) {
                val = Math.pow(num, Double.valueOf(String.valueOf(colVal)));
            }
            set(i, colIndex, (V) val);
        }
        return this;
    }

    /**
     * 给指定列进行pow,返回指定列的Double值
     * @param num 底数
     * @param cols 指数所在列名
     */
    public DataFrame<V> pow(Double num, List cols) {
        for (Object col : cols) {
            pow(num, col);
        }

        return this;
    }

    /**
     * 给指定列进行pow,返回指定列的int值
     * @param num 底数
     * @param col 指数所在列名
     */
    public DataFrame<V> powInt(Double num, Object col) {
        Integer colIndex = getColIndex(col);
        if (colIndex == null) {
            throw new IllegalArgumentException("列名不存在" + col);
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

    /**
     * 给指定列进行pow,返回指定列的int值
     * @param num 底数
     * @param cols 指数所在列名
     */
    public DataFrame<V> powInt(Double num, List cols) {
        for (Object col : cols) {
            powInt(num, col);
        }

        return this;
    }


    /**
     * 判断某行的指定列colList的null值是否大于condition，并改变改行col列的值为col+change
     * @param colList 指定列
     * @param col 要改变的列
     * @param condition 判断null数量条件
     * @param change 改变值
     */
    public DataFrame<V> changeOnNaCondition(List colList, String col, int condition, double change) {
        Integer colIndex = getColIndex(col);
        if (colIndex == null) {
            throw new IllegalArgumentException("列名不存在" + col);
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
     * @param col 指定列
     */
    public V max(V col) {
        V max = getVal(col, -1);
        return max;
    }

    /**
     * 获取指定列最小值
     * @param col 指定列
     */
    public V min(V col) {
        V min = getVal(col, 1);
        return min;
    }

    private V getVal(V col, final int sort) {
        Integer colIndex = getColIndex(col);
        if (colIndex == null) {
            throw new IllegalArgumentException("列名不存在" + col);
        }
        List<V> columnList = column(colIndex);

        List<V> collect = columnList.stream().filter(x -> x != null).collect(Collectors.toList());

        if (collect == null) {
            return null;
        }

        Collections.sort(collect, new Comparator<V> () {
            @Override
            public int compare(V o1, V o2) {
                int result = 0;
                Comparable v1 = Comparable.class.cast(o1);
                Comparable v2 = Comparable.class.cast(o2);

                result = v1.compareTo(v2) * sort;
                return result;
            }
        });

        return collect.get(0);
    }

    /**
     * 删除指定列
     * @param cols 指定列
     */
    public DataFrame<V> drop(final Object ... cols) {
        return drop(indices(cols));
    }

    /**
     * A function that is applied to objects (rows or values)
     * in a {@linkplain DataFrame data frame}.
     *
     * <p>Implementors define {@link #apply(Object, int)} to perform
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
        O apply(I value, int side);
    }

    /**
     * A function that converts {@linkplain DataFrame data frame}
     * rows to index or group keys.
     *
     * <p>Implementors define {@link #apply(Object, int)} to accept
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
     * other the other data frame
     * join the join type
     * cols the indices of the columns to use as the join key
     */
//    public final DataFrame<V> joinOn(final DataFrame<V> other, final JoinType join, final Integer ... cols) {
//        return Combining.joinOn(this, other, join, cols);
//    }

    /**
     * Return a new data frame created by performing a join of this
     * data frame with the argument using the specified join type and
     * the column values as the join key.
     *
     * right the other data frame
     * join the join type
     * colKey the names of the columns to use as the join key
     * the result of the join operation as a new data frame
     */
    public final DataFrame<V> joinOn(final DataFrame<V> right, final JoinType join, final String colKey) {
//        return joinOn(right, join, indices(colKey));
        return Combining.joinOn(this, right, join, colKey);
    }


    /**
     * 排序
     * @param cols 指定列
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
            Integer colIndex = getColIndex(names.get(i));
            if (colIndex == null) {
                throw new IllegalArgumentException("column name: '" + names.get(i) +  "' is not exist");
            }
            indices[i] = colIndex;
        }
        return indices;
    }

    /**
     * 对指定列去重
     * @param cols 列名
     */
    public DataFrame<V> unique(final Object ... cols) {
        return unique(indices(cols));
    }

    /**
     * 对指定列去重
     * @param cols 列索引
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
