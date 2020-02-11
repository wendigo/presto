/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.server.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.prestosql.client.Column;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class RowIterables
        extends AbstractIterator<List<Object>>
        implements Iterable<List<Object>>
{
    private final ConnectorSession session;
    private final List<Type> types;
    private final List<Column> columns;
    private final Deque<Page> pages;
    private final List<TypeSerializationException> exceptions = new ArrayList<>();
    private final int totalRows;

    private Page currentPage;
    private int rowPosition = -1;
    private int inPageIndex;

    private RowIterables(ConnectorSession session, List<Type> types, List<Column> columns, List<Page> pages)
    {
        this.session = requireNonNull(session, "session is null");
        this.types = requireNonNull(types, "types is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.pages = new ArrayDeque<>(requireNonNull(pages, "pages is null"));
        this.totalRows = countRows(pages);
        this.currentPage = this.pages.pollFirst();
        this.inPageIndex = 0;

        verify(this.types.size() == this.columns.size(), "columns and types sizes mismatch");
    }

    public boolean isEmpty()
    {
        return totalRows == 0;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public int getTotalRows()
    {
        return totalRows;
    }

    public Long getUpdateCount()
    {
        if (totalRows == 1 && columns.size() == 1) {
            Page firstPage = pages.peekFirst();
            checkNotNull(firstPage, "firstPage is null");

            Block block = firstPage.getBlock(0);
            Number value = (Number) types.get(0).getObjectValue(session, block, 0);

            if (value != null) {
                return value.longValue();
            }
        }

        return null;
    }

    @Override
    protected List<Object> computeNext()
    {
        loop:
        while (true) {
            inPageIndex++;

            if (currentPage == null || (pages.isEmpty() && inPageIndex >= currentPage.getPositionCount())) {
                return endOfData();
            }
            else if (inPageIndex >= currentPage.getPositionCount()) {
                currentPage = pages.pollFirst();
                checkNotNull(currentPage, "currentPage is null");
                inPageIndex = 0;
            }

            rowPosition++;
            List<Object> values = new ArrayList<>(columns.size());

            for (int channel = 0; channel < currentPage.getChannelCount(); channel++) {
                Type type = types.get(channel);
                Block block = currentPage.getBlock(channel);

                try {
                    values.add(channel, type.getObjectValue(session, block, inPageIndex));
                }
                catch (Throwable throwable) {
                    handleException(rowPosition, channel, throwable);
                    continue loop;
                }
            }

            return unmodifiableList(values);
        }
    }

    private void handleException(final int row, final int column, final Throwable cause)
    {
        // columns and rows are 0-indexed
        TypeSerializationException exception = new TypeSerializationException(
                cause,
                columns.get(column).getName(),
                columns.get(column).getType(),
                row + 1,
                column + 1);

        exceptions.add(exception);
    }

    public List<TypeSerializationException> getSerializationExceptions()
    {
        return ImmutableList.copyOf(exceptions);
    }

    @Override
    public Iterator<List<Object>> iterator()
    {
        return this;
    }

    private static int countRows(List<Page> pages)
    {
        return pages.stream()
                .map(Page::getPositionCount)
                .reduce(Integer::sum)
                .orElse(0);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("types", types)
                .add("columns", columns)
                .add("rowsCount", getTotalRows())
                .toString();
    }

    public static RowIterables empty(ConnectorSession session)
    {
        return new RowIterables(session, ImmutableList.of(), ImmutableList.of(), ImmutableList.of());
    }

    public static class Builder
    {
        private final ConnectorSession connectorSession;
        private List<Page> pages = Lists.newArrayList();
        private List<Type> types = emptyList();
        private List<Column> columns = emptyList();

        public Builder(ConnectorSession connectorSession)
        {
            this.connectorSession = requireNonNull(connectorSession, "connectorSession is null");
        }

        public Builder add(Page page)
        {
            pages.add(page);
            return this;
        }

        public Builder withColumns(List<Column> columns)
        {
            if (columns != null) {
                this.columns = columns;
            }

            return this;
        }

        public Builder withTypes(List<Type> types)
        {
            if (types != null) {
                this.types = types;
            }

            return this;
        }

        public Builder withSingleBooleanResult(boolean result)
        {
            BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(null, 1);
            BOOLEAN.writeBoolean(blockBuilder, result);
            pages = ImmutableList.of(new Page(blockBuilder.build()));
            types = ImmutableList.of(BOOLEAN);

            return this;
        }

        public RowIterables build()
        {
            return new RowIterables(
                    connectorSession,
                    ImmutableList.copyOf(types),
                    ImmutableList.copyOf(columns),
                    ImmutableList.copyOf(pages));
        }
    }

    public static Builder builder(ConnectorSession connectorSession)
    {
        return new Builder(connectorSession);
    }
}
