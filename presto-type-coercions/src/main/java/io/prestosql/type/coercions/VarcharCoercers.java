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
package io.prestosql.type.coercions;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.type.coercions.TypeCoercers.isNumber;
import static io.prestosql.type.coercions.TypeCoercers.longCanBeCasted;
import static java.lang.String.format;

public class VarcharCoercers
{
    private VarcharCoercers() {}

    public static TypeCoercer<VarcharType, ?> createCoercer(VarcharType sourceType, Type targetType)
    {
        if (isNumber(targetType)) {
            return TypeCoercer.create(sourceType, targetType, VarcharCoercers::coerceVarcharToInteger);
        }

        throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s", sourceType, targetType));
    }

    private static void coerceVarcharToInteger(VarcharType sourceType, Type targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        long value = Long.parseLong(sourceType.getSlice(block, position).toStringUtf8());

        if (longCanBeCasted(value, sourceType, targetType)) {
            targetType.writeLong(blockBuilder, value);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s", sourceType, targetType));
        }
    }
}
