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
import io.github.yufeixuan.DataFrame.SortDirection;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class Sorting {
    public static <V> DataFrame<V> sort(
            final DataFrame<V> df, final Map<Integer, SortDirection> cols) {
            final Comparator<List<V>> comparator = new Comparator<List<V>>() {
                @Override
                @SuppressWarnings("unchecked")
                public int compare(final List<V> r1, final List<V> r2) {
                    int result = 0;
                    for (final Map.Entry<Integer, SortDirection> col : cols.entrySet()) {
                        final int c = col.getKey();
                        final Comparable<V> v1 = Comparable.class.cast(r1.get(c));
                        final V v2 = r2.get(c);
                        result = v1.compareTo(v2);
                        result *= col.getValue() == SortDirection.DESCENDING ? -1 : 1;
                        if (result != 0) {
                            break;
                        }
                    }
                    return result;
                }
            };
        return sort(df, comparator);
    }

    public static <V> DataFrame<V> sort(final DataFrame<V> df, final Comparator<List<V>> comparator) {
        final DataFrame<V> sorted = new DataFrame<V>(df.getColumns());
        final Comparator<Integer> cmp = new Comparator<Integer>() {
            @Override
            public int compare(final Integer r1, final Integer r2) {
                return comparator.compare(df.row(r1),  df.row(r2));
            }
        };

        final Integer[] rows = new Integer[df.length()];
        for (int r = 0; r < df.length(); r++) {
            rows[r] = r;
        }
        Arrays.sort(rows, cmp);

        for (final Integer r : rows) {
            sorted.append(df.row(r));
        }

        return sorted;
    }
}
