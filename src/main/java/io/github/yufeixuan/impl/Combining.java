/*
 * Joinery -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.github.yufeixuan.impl;

import io.github.yufeixuan.DataFrame;
import io.github.yufeixuan.DataFrame.JoinType;
import io.github.yufeixuan.DataFrame.KeyFunction;

import java.util.*;

public class Combining {
    public static <V> DataFrame<V> join(final DataFrame<V> left, final DataFrame<V> right, final JoinType how, final String joinKey, final KeyFunction<V> on) {
        final Integer lColIndex = left.getColIndex(joinKey);
        final Integer rColIndex = right.getColIndex(joinKey);

        if (lColIndex == null || rColIndex == null) {
            throw new IllegalArgumentException("joinKey不存在: " + joinKey);
        }
        final Map<Object, List<V>> leftMap = new LinkedHashMap<>();
        final Map<Object, List<V>> rightMap = new LinkedHashMap<>();

        for (final List<V> row : left) {
            final Object key = on.apply(row, 0);
            if (leftMap.put(key, row) != null) {
                throw new IllegalArgumentException("generated key is not unique: " + key);
            }
        }

        for (final List<V> row : right) {
            final Object key = on.apply(row, 1);
            if (rightMap.put(key, row) != null) {
                throw new IllegalArgumentException("generated key is not unique: " + key);
            }
        }

        final LinkedList<Object> columns = new LinkedList<>(how != JoinType.RIGHT ? left.getColumns() : right.getColumns());
        for (Object column : how != JoinType.RIGHT ? right.getColumns() : left.getColumns()) {
            final int index = columns.indexOf(column);
            if (index >= 0) {
                if (column instanceof List) {
                    @SuppressWarnings("unchecked")
                    final List<Object> l1 = List.class.cast(columns.get(index));
                    l1.add(how != JoinType.RIGHT ? "left" : "right");
                    @SuppressWarnings("unchecked")
                    final List<Object> l2= List.class.cast(column);
                    l2.add(how != JoinType.RIGHT ? "right" : "left");
                } else {
                    columns.set(index, String.format("%s_%s", columns.get(index), how != JoinType.RIGHT ? "left" : "right"));
                    column = String.format("%s_%s", column, how != JoinType.RIGHT ? "right" : "left");
                }
            }
            columns.add(column);
        }

        final DataFrame<V> df = new DataFrame<>(columns);
        for (final Map.Entry<Object, List<V>> entry : how != JoinType.RIGHT ? leftMap.entrySet() : rightMap.entrySet()) {
            final List<V> tmp = new ArrayList<>(entry.getValue());
            final List<V> row = how != JoinType.RIGHT ? rightMap.get(entry.getKey()) : leftMap.get(entry.getKey());
            if (row != null || how != JoinType.INNER) {
                tmp.addAll(row != null ? row : Collections.<V>nCopies(right.getColumns().size(), null));
                df.append(tmp);
            }
        }

        if (how == JoinType.OUTER) {
            for (final Map.Entry<Object, List<V>> entry : how != JoinType.RIGHT ? rightMap.entrySet() : leftMap.entrySet()) {
                final List<V> row = how != JoinType.RIGHT ? leftMap.get(entry.getKey()) : rightMap.get(entry.getKey());
                if (row == null) {
                    final List<V> tmp = new ArrayList<>(Collections.<V>nCopies(
                        how != JoinType.RIGHT ? left.getColumns().size() : right.getColumns().size(), null));
                    tmp.set(lColIndex, entry.getValue().get(rColIndex));
                    tmp.addAll(entry.getValue());
                    df.append(tmp);
                }
            }
        }

        df.rename(String.format("%s_%s", joinKey, "left"), joinKey).drop(String.format("%s_%s", joinKey, "right"));

        return df;
    }


    public static <V> DataFrame<V> joinOn(final DataFrame<V> left, final DataFrame<V> right, final JoinType how, final String joinKey) {
        return join(left, right, how, joinKey, new KeyFunction<V>() {
            @Override
            public Object apply(final List<V> value, final int side) {
                final List<V> key = new ArrayList<>();
                Integer colIndex = null;
                if (side == 0) {
                    colIndex = left.getColIndex(joinKey);
                }
                else {
                    colIndex = right.getColIndex(joinKey);
                }

                if (colIndex == null) {
                    throw new IllegalArgumentException("joinKey不存在: " + joinKey);
                }
                key.add(value.get(colIndex));

                return Collections.unmodifiableList(key);
            }
        });
    }
}
