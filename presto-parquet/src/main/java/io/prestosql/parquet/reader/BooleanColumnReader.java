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
package io.prestosql.parquet.reader;

import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

public class BooleanColumnReader
        extends PrimitiveColumnReader
{
    public BooleanColumnReader(RichColumnDescriptor columnDescriptor, Type sourceType, Type targetType)
    {
        super(columnDescriptor, sourceType, targetType);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            sourceType.writeBoolean(blockBuilder, valuesReader.readBoolean());
        }
        else if (isValueNull()) {
            blockBuilder.appendNull();
        }
    }

    @Override
    protected void skipValue()
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            valuesReader.readBoolean();
        }
    }
}
