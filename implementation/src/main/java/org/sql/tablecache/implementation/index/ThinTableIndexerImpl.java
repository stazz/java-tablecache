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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.qi4j.api.util.Iterables;
import org.sql.tablecache.api.callbacks.ThinIndexingKeyProvider;
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
        private final Iterable<TableRow> _realIterable;

        private ThinTableAccessor( CacheInfo cacheInfo, Iterable<TableRow> realIterable )
        {
            this._cacheInfo = cacheInfo;
            this._realIterable = realIterable;
        }

        @Override
        public Iterator<TableRow> iterator()
        {
            return new Iterator<TableRow>()
            {
                private final Iterator<TableRow> _realIterator = _realIterable.iterator();

                @Override
                public boolean hasNext()
                {
                    Lock lock = _cacheInfo.getAccessLock().readLock();
                    lock.lock();
                    try
                    {
                        return _realIterator.hasNext();
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
                        return _realIterator.next();
                    }
                    finally
                    {
                        lock.unlock();
                    }
                }

                @Override
                public void remove()
                {
                    throw new UnsupportedOperationException( "Indexing is read-only." );
                }
            };
        }

    }

    private final Map<Object, Set<TableRow>> _rows;
    private final CacheInfo _cacheInfo;
    private final ThinIndexingKeyProvider _keyProvider;

    public ThinTableIndexerImpl( CacheInfo cacheInfo, ThinIndexingKeyProvider provider )
    {
        this._rows = new HashMap<Object, Set<TableRow>>();
        this._cacheInfo = cacheInfo;
        this._keyProvider = provider;
    }

    @Override
    public Iterable<TableRow> getRows( Object indexingColumnValue )
    {
        Lock lock = _cacheInfo.getAccessLock().readLock();
        lock.lock();
        try
        {
            Set<TableRow> rows = this._rows.get( indexingColumnValue );
            return rows == null ? Collections.EMPTY_SET : new ThinTableAccessor( this._cacheInfo, rows );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public Boolean hasRows( Object indexingColumnValue )
    {
        Lock lock = _cacheInfo.getAccessLock().readLock();
        lock.lock();
        try
        {
            return this._rows.containsKey( indexingColumnValue );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public TableAccessor getRows()
    {
        return new ThinTableAccessor( this._cacheInfo, Iterables.flattenIterables( this._rows.values() ) );
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
