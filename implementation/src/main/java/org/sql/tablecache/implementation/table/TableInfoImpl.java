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

package org.sql.tablecache.implementation.table;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.sql.tablecache.api.table.TableInfo;

public class TableInfoImpl
    implements TableInfo
{
    private final String _schemaName;
    private final String _tableName;
    private final List<String> _columns;
    private final Map<String, Integer> _columnIndices;
    private final Set<String> _pkColumns;

    public TableInfoImpl( String schemaName, String tableName, List<String> columns, Set<String> pkColumns )
    {
        this._schemaName = schemaName;
        this._tableName = tableName;
        this._columns = Collections.unmodifiableList( columns );
        Map<String, Integer> columnIndices = new HashMap<String, Integer>();
        this._pkColumns = Collections.unmodifiableSet( pkColumns );
        for( Integer idx = 0; idx < columns.size(); ++idx )
        {
            String col = columns.get( idx );
            if( !columnIndices.containsKey( col ) )
            {
                columnIndices.put( col, idx );
            }
            else
            {
                throw new IllegalArgumentException( "Duplicate column name: " + col + "." );
            }
        }
        this._columnIndices = Collections.unmodifiableMap( columnIndices );
    }

    @Override
    public Map<String, Integer> getColumnIndices()
    {
        return this._columnIndices;
    }

    @Override
    public Set<String> getPkColumns()
    {
        return this._pkColumns;
    }

    @Override
    public List<String> getColumns()
    {
        return this._columns;
    }

    @Override
    public String getSchemaName()
    {
        return this._schemaName;
    }

    @Override
    public String getTableName()
    {
        return this._tableName;
    }

}