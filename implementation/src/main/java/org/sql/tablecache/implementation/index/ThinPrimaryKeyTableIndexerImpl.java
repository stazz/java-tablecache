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

import java.util.HashMap;
import java.util.Map;

import org.sql.tablecache.api.callbacks.ThinIndexingKeyProvider;
import org.sql.tablecache.api.index.ThinPrimaryKeyTableIndexer;
import org.sql.tablecache.api.table.TableAccessor;
import org.sql.tablecache.api.table.TableRow;
import org.sql.tablecache.implementation.cache.TableCacheImpl.CacheInfo;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public class ThinPrimaryKeyTableIndexerImpl extends AbstractTableIndexer
    implements ThinPrimaryKeyTableIndexer
{

    private final Map<Object, TableRow> _rows;
    private final CacheInfo _cacheInfo;
    private final ThinIndexingKeyProvider _pkProvider;

    public ThinPrimaryKeyTableIndexerImpl( CacheInfo cacheInfo, ThinIndexingKeyProvider provider )
    {
        this._rows = new HashMap<Object, TableRow>();
        this._cacheInfo = cacheInfo;
        this._pkProvider = provider;
    }

    @Override
    public TableRow getRow( Object pk )
    {
        return this._rows.get( pk );
    }

    @Override
    public Boolean hasRow( Object pk )
    {
        return this._rows.containsKey( pk );
    }

    @Override
    public void insertOrUpdateRow( TableRow newRow )
    {
        Object pk = this._pkProvider.createThinIndexingKey( newRow );
        // Write-locking is not required as the table cache should do it
        this._rows.put( pk, newRow );
    }

    @Override
    public TableAccessor getRows()
    {
        return new TableAccessorImpl( this._cacheInfo, this._rows.values() );
    }
}
