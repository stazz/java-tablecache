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
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.sql.tablecache.api.callbacks.ThinIndexingPKProvider;
import org.sql.tablecache.api.index.ThinTableIndexer;
import org.sql.tablecache.api.table.TableAccessor;
import org.sql.tablecache.api.table.TableRow;
import org.sql.tablecache.implementation.cache.TableCacheImpl.CacheInfo;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public class ThinTableIndexerImpl extends AbstractTableIndexer
    implements ThinTableIndexer
{

    private static class ThinTableAccessor
        implements TableAccessor
    {
        private final CacheInfo _cacheInfo;
        private final Map<Object, TableRow> _rows;

        private ThinTableAccessor( CacheInfo cacheInfo, Map<Object, TableRow> rows )
        {
            this._cacheInfo = cacheInfo;
            this._rows = rows;
        }

        @Override
        public Iterator<TableRow> iterator()
        {
            return new Iterator<TableRow>()
            {
                private Iterator<TableRow> _actualIterator = _rows.values().iterator();

                @Override
                public boolean hasNext()
                {
                    Lock lock = _cacheInfo.getAccessLock().readLock();
                    lock.lock();
                    try
                    {
                        return _actualIterator.hasNext();
                    }
                    finally
                    {
                        lock.unlock();
                    }
                }

                @Override
                public TableRow next()
                {
                    Lock lock = _cacheInfo.getAccessLock().readLock();
                    lock.lock();
                    try
                    {
                        return _actualIterator.next();
                    }
                    finally
                    {
                        lock.unlock();
                    }
                }

                @Override
                public void remove()
                {
                    throw new UnsupportedOperationException( "Removing rows from table index is not possible." );
                }
            };
        }

    }

    private final Map<Object, TableRow> _rows;
    private final CacheInfo _cacheInfo;
    private final ThinIndexingPKProvider _pkProvider;

    public ThinTableIndexerImpl( CacheInfo cacheInfo, ThinIndexingPKProvider provider )
    {
        this._rows = new HashMap<Object, TableRow>();
        this._cacheInfo = cacheInfo;
        this._pkProvider = provider;
    }

    @Override
    public TableRow getRow( Object pk )
    {
        Lock lock = _cacheInfo.getAccessLock().readLock();
        lock.lock();
        try
        {
            return this._rows.get( pk );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public Boolean hasRow( Object pk )
    {
        Lock lock = _cacheInfo.getAccessLock().readLock();
        lock.lock();
        try
        {
            return this._rows.containsKey( pk );
        }
        finally
        {
            lock.unlock();
        }
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
        return new ThinTableAccessor( this._cacheInfo, this._rows );
    }
}
