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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.sql.tablecache.api.index.TableIndexer;
import org.sql.tablecache.api.table.Table;
import org.sql.tablecache.api.table.TableInfo;
import org.sql.tablecache.api.table.TableRow;
import org.sql.tablecache.implementation.index.AbstractTableIndexer;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public class TableImpl
    implements Table
{

    private final TableInfo _tableInfo;
    private final Map<String, TableIndexer> _indexers;

    public TableImpl( TableInfo tableInfo, Map<String, TableIndexer> indexers )
    {
        this._tableInfo = tableInfo;
        this._indexers = Collections.unmodifiableMap( indexers );
    }

    @Override
    public TableInfo getTableInfo()
    {
        return this._tableInfo;
    }

    @Override
    public TableIndexer getDefaultIndexer()
    {
        return this._indexers.get( null );
    }

    @Override
    public <AccessorType extends TableIndexer> AccessorType getDefaultIndexer( Class<AccessorType> accessorClass )
    {
        return accessorClass.cast( this.getDefaultIndexer() );
    }

    @Override
    public <AccessorType extends TableIndexer> AccessorType getIndexer( Class<AccessorType> accessorClass,
        String indexName )
    {
        return accessorClass.cast( this._indexers.get( indexName ) );
    }

    @Override
    public TableRow createRow( ResultSet row )
        throws SQLException
    {
        List<String> cols = this._tableInfo.getColumns();
        Object[] result = new Object[cols.size()];
        // Populate row array in a order as specified by column name list
        int idx = 0;
        for( String col : cols )
        {
            result[idx] = row.getObject( col );
            ++idx;
        }

        return new TableRowImpl( this._tableInfo, result );
    }

    @Override
    public Map<String, TableIndexer> getIndexers()
    {
        return this._indexers;
    }

    @Override
    public void insertOrUpdateRows( TableRow... rows )
    {
        for( TableRow row : rows )
        {
            if( this._tableInfo.equals( row.getTableInfo() ) )
            {
                for( TableIndexer indexer : this._indexers.values() )
                {
                    ((AbstractTableIndexer) indexer).insertOrUpdateRow( row );
                }
            }
        }
    }

}
