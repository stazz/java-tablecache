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

package org.sql.tablecache.implementation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.sql.tablecache.api.PrimaryKeyInfoProvider.PrimaryKeyInfo;
import org.sql.tablecache.api.TableInfo;
import org.sql.tablecache.api.TableRow;

public class TableInfoImpl
    implements TableInfo
{
    private final String _schemaName;
    private final String _tableName;
    private final List<String> _columns;
    private final Map<String, Integer> _columnIndices;
    private final Map<String, Integer> _pkIndices;
    private final PrimaryKeyInfo _pkInfo;

    public TableInfoImpl( String schemaName, String tableName, List<String> columns, PrimaryKeyInfo pkInfo )
    {
        this._schemaName = schemaName;
        this._tableName = tableName;
        this._columns = columns;
        this._pkInfo = pkInfo;
        this._columnIndices = new HashMap<String, Integer>();
        Set<String> pkColumns = this._pkInfo.getKeyNames();
        this._pkIndices = new HashMap<String, Integer>( pkColumns.size() );
        Integer pkIdx = 0;
        for( Integer idx = 0; idx < columns.size(); ++idx )
        {
            String col = columns.get( idx );
            if( !this._columnIndices.containsKey( col ) )
            {
                this._columnIndices.put( col, idx );
                if( pkColumns.contains( col ) )
                {
                    this._pkIndices.put( col, pkIdx );
                    ++pkIdx;
                }
            }
            else
            {
                throw new IllegalArgumentException( "Duplicate column name: " + col + "." );
            }
        }
    }

    @Override
    public Map<String, Integer> getColumnIndices()
    {
        return this._columnIndices;
    }

    @Override
    public Set<String> getPkColumns()
    {
        return this._pkInfo.getKeyNames();
    }

    @Override
    public Map<String, Integer> getPkIndices()
    {
        return this._pkIndices;
    }

    @Override
    public List<String> getColumns()
    {
        return this._columns;
    }

    @Override
    public Object createThinIndexPK( TableRow row )
    {
        return this._pkInfo.createThinIndexingMultiKey( this, row );
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

    @Override
    public Boolean useBroadIndexing()
    {
        return this._pkInfo.useBroadIndexing();
    }

    @Override
    public Boolean useThinIndexing()
    {
        return this._pkInfo.useThinIndexing();
    }
}