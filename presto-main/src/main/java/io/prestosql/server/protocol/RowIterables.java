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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.prestosql.client.Column;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

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
    private final List<Consumer<TypeSerializationException>> exceptionConsumers;

    private List<Type> types;
    private List<Column> columns;
    private List<Page> pages;

    private int rowsCount;
    private int pagePosition = -1;
    private int rowPosition = -1;

    private Page currentPage;
    private int pageIndex;

    private RowIterables(ConnectorSession session, List<Consumer<TypeSerializationException>> exceptionConsumers, List<Type> types, List<Column> columns, List<Page> pages)
    {
        this.session = requireNonNull(session, "session is null");
        this.types = requireNonNull(types, "types is null");
        this.exceptionConsumers = requireNonNull(exceptionConsumers, "exceptionConsumers is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.pages = requireNonNull(pages, "pages is null");
        this.rowsCount = countRows(pages);
        this.currentPage = null;
        this.pageIndex = 0;

        verify(this.types.size() == this.columns.size(), "columns and types sizes mismatch");
    }

    public boolean isEmpty()
    {
        return rowsCount == 0;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public int getRowsCount()
    {
        return rowsCount;
    }

    public Long getUpdateCount()
    {
        if (rowsCount == 1 && columns.size() == 1) {
            Block block = pages.get(0).getBlock(0);
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
        pagePosition++;

        if (rowPosition >= rowsCount || pageIndex >= pages.size()) {
            return endOfData();
        }

        currentPage = pages.get(pageIndex);

        if (pagePosition >= currentPage.getPositionCount()) {
            if (pageIndex < pages.size()) {
                pagePosition = 0;
                pageIndex++;
                return computeNext();
            }
            else {
                return endOfData();
            }
        }

        rowPosition++;

        List<Object> values = new ArrayList<>(columns.size());

        for (int channel = 0; channel < currentPage.getChannelCount(); channel++) {
            Type type = types.get(channel);
            Block block = currentPage.getBlock(channel);

            try {
                values.add(channel, type.getObjectValue(session, block, pagePosition));
            }
            catch (Throwable throwable) {
                values.add(channel, null);

                final int currentChannel = channel;
                exceptionConsumers.forEach(consumer -> consumer.accept(new TypeSerializationException(
                        throwable,
                        columns.get(currentChannel),
                        pagePosition,
                        currentChannel)));
            }
        }

        return unmodifiableList(values);
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

    public static class Builder
    {
        private final ConnectorSession connectorSession;
        private final List<Consumer<TypeSerializationException>> listeners;

        private List<Page> pages = Lists.newArrayList();
        private List<Type> types = emptyList();
        private List<Column> columns = emptyList();

        public Builder(ConnectorSession connectorSession)
        {
            this.connectorSession = requireNonNull(connectorSession, "connectorSession is null");
            this.listeners = Lists.newArrayList();
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

        public Builder withExceptionListener(Consumer<TypeSerializationException> listener)
        {
            listeners.add(requireNonNull(listener, "listener is null"));
            return this;
        }

        public RowIterables build()
        {
            return new RowIterables(
                    connectorSession,
                    ImmutableList.copyOf(listeners),
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
