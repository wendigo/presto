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

import static java.lang.String.format;

public class TypeSerializationException
        extends Throwable
{
    private final Throwable cause;
    private final String columnName;
    private final String columnType;
    private final int row;
    private final int column;

    public TypeSerializationException(Throwable cause, String columnName, String columnType, int row, int column)
    {
        super(format("Could not serialize '%s' type value at row: %d, column: %d ('%s')", columnType, row, column, columnName), cause);
        this.cause = cause;
        this.columnName = columnName;
        this.columnType = columnType;
        this.row = row;
        this.column = column;
    }

    public Throwable getCause()
    {
        return cause;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public String getColumnType()
    {
        return columnType;
    }

    public int getColumn()
    {
        return column;
    }

    public int getRow()
    {
        return row;
    }
}
