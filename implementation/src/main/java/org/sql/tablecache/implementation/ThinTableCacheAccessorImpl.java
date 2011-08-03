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
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.sql.tablecache.api.TableAccessor;
import org.sql.tablecache.api.TableIndexer.ThinTableIndexer;
import org.sql.tablecache.api.TableInfo;
import org.sql.tablecache.implementation.TableCacheImpl.CacheInfo;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public class ThinTableCacheAccessorImpl extends AbstractTableIndexer
    implements ThinTableIndexer
{

    private static class ThinTableAccessor
        implements TableAccessor
    {
        private final CacheInfo _cacheInfo;
        private final Map<Object, Object[]> _rows;

        private ThinTableAccessor( CacheInfo cacheInfo, Map<Object, Object[]> rows )
        {
            this._cacheInfo = cacheInfo;
            this._rows = rows;
        }

        @Override
        public Iterator<Object[]> iterator()
        {
            return new Iterator<Object[]>()
            {
                private Iterator<Object[]> _actualIterator = _rows.values().iterator();

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
                public Object[] next()
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

    private final Map<Object, Object[]> _rows;
    private final CacheInfo _cacheInfo;

    public ThinTableCacheAccessorImpl( CacheInfo cacheInfo )
    {
        this._rows = new HashMap<Object, Object[]>();
        this._cacheInfo = cacheInfo;
    }

    @Override
    public Object[] getRow( Object pk )
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
    protected void insertOrUpdateRow( Object[] newRow )
    {
        // TODO validate new row
        Object pk = this._cacheInfo.getTableInfo().createThinIndexPK( newRow );
        // Write-locking is not required as the table cache should do it
        this._rows.put( pk, newRow );
    }

    @Override
    public TableAccessor getRows()
    {
        return new ThinTableAccessor( this._cacheInfo, this._rows );
    }
}
