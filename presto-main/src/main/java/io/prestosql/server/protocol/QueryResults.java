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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.client.Column;
import io.prestosql.client.QueryError;
import io.prestosql.client.StatementStats;
import io.prestosql.client.Warning;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class QueryResults
{
    private final String id;
    private final URI infoUri;
    private final URI partialCancelUri;
    private final URI nextUri;
    private final List<Column> columns;
    private final RowIterables data;
    private final StatementStats stats;
    private final QueryError error;
    private final List<Warning> warnings;
    private final String updateType;
    private final Long updateCount;

    public QueryResults(
            String id,
            URI infoUri,
            URI partialCancelUri,
            URI nextUri,
            List<Column> columns,
            RowIterables data,
            StatementStats stats,
            QueryError error,
            List<Warning> warnings,
            String updateType,
            Long updateCount)
    {
        this.id = requireNonNull(id, "id is null");
        this.infoUri = requireNonNull(infoUri, "infoUri is null");
        this.partialCancelUri = partialCancelUri;
        this.nextUri = nextUri;
        this.columns = (columns != null) ? ImmutableList.copyOf(columns) : null;
        this.data = requireNonNull(data, "data is empty");
        checkArgument(data.isEmpty() || columns != null, "data present without columns");
        this.stats = requireNonNull(stats, "stats is null");
        this.error = error;
        this.warnings = ImmutableList.copyOf(requireNonNull(warnings, "warnings is null"));
        this.updateType = updateType;
        this.updateCount = updateCount;
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    @JsonProperty
    public URI getInfoUri()
    {
        return infoUri;
    }

    @Nullable
    @JsonProperty
    public URI getPartialCancelUri()
    {
        return partialCancelUri;
    }

    @Nullable
    @JsonProperty
    public URI getNextUri()
    {
        return nextUri;
    }

    @Nullable
    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @Nullable
    @JsonProperty
    public RowIterables getData()
    {
        return data;
    }

    @JsonProperty
    public StatementStats getStats()
    {
        return stats;
    }

    @Nullable
    @JsonProperty
    public QueryError getError()
    {
        return error;
    }

    @JsonProperty
    public List<Warning> getWarnings()
    {
        return warnings;
    }

    @Nullable
    @JsonProperty
    public String getUpdateType()
    {
        return updateType;
    }

    @Nullable
    @JsonProperty
    public Long getUpdateCount()
    {
        return updateCount;
    }

    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("infoUri", infoUri)
                .add("partialCancelUri", partialCancelUri)
                .add("nextUri", nextUri)
                .add("columns", columns)
                .add("hasData", data != null)
                .add("stats", stats)
                .add("error", error)
                .add("updateType", updateType)
                .add("updateCount", updateCount)
                .toString();
    }
}
