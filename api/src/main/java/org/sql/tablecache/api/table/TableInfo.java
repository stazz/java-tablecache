/*
 * Copyright (c) 2011, Stanislav Muhametsin. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.sql.tablecache.api.table;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public interface TableInfo
{

    public Map<String, Integer> getColumnIndices();

    public Set<String> getPkColumns();

    // TODO ColumnInfo (isPK, name, type)
    /**
     * Returns the names of all the columns of this table. It is guaranteed that this list will be essentially a set.
     * Additionally, for all column names, the value for the column is at same index in row, as it is in the column list
     * returned by this method.
     * 
     * @return The names of all the columns of this table.
     */
    public List<String> getColumns();

    public String getTableName();

    public String getSchemaName();

    /**
     * Returns column types as one of the static values in {@link Types} class. Order is the same as in
     * {@link #getColumns()} method.
     * 
     * @return Column types as one of the static values in {@link Types} class.
     */
    public List<Integer> getColumnTypes();

}