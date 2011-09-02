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

package org.sql.tablecache.implementation.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.qi4j.api.util.Iterables;
import org.sql.tablecache.api.callbacks.ThinIndexingKeyProvider;
import org.sql.tablecache.api.index.ThinTableIndexer;
import org.sql.tablecache.api.table.TableAccessor;
import org.sql.tablecache.api.table.TableRow;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public class ThinTableIndexerImpl extends AbstractTableIndexer
    implements ThinTableIndexer
{
    private final Map<Object, Set<TableRow>> _rows;
    private final ThinIndexingKeyProvider _keyProvider;

    public ThinTableIndexerImpl( ThinIndexingKeyProvider provider )
    {
        this._rows = new HashMap<Object, Set<TableRow>>();
        this._keyProvider = provider;
    }

    @Override
    public Iterable<TableRow> getRows( Object indexingColumnValue )
    {
        Set<TableRow> rows = this._rows.get( indexingColumnValue );
        return rows == null ? Collections.EMPTY_SET : new TableAccessorImpl( rows );
    }

    @Override
    public Boolean hasRows( Object indexingColumnValue )
    {
        return this._rows.containsKey( indexingColumnValue );
    }

    @Override
    public TableAccessor getRows()
    {
        return new TableAccessorImpl( Iterables.flattenIterables( this._rows.values() ) );
    }

    @Override
    public void insertOrUpdateRow( TableRow row )
    {
        Object pk = this._keyProvider.createThinIndexingKey( row );
        // Write-locking is not required as the table cache should do it
        Set<TableRow> rows = this._rows.get( pk );
        if( rows == null )
        {
            rows = new HashSet<TableRow>();
            this._rows.put( pk, rows );
        }
        rows.add( row );
    }

}
