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
package io.prestosql.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class SerializationError
{
    private final int row;
    private final int column;
    private final String columnName;
    private final String columnType;
    private final String error;

    @JsonCreator
    public SerializationError(
            @JsonProperty("row") int row,
            @JsonProperty("column") int column,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") String columnType,
            @JsonProperty("error") String error)
    {
        this.row = row;
        this.column = column;
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.error = requireNonNull(error, "error is null");
    }

    @JsonProperty
    @Nullable
    public int getRow()
    {
        return row;
    }

    @JsonProperty
    @Nullable
    public int getColumn()
    {
        return column;
    }

    @JsonProperty
    @Nullable
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    @Nullable
    public String getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    @Nullable
    public String getError()
    {
        return error;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("row", row)
                .add("column", column)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("error", error)
                .toString();
    }
}
