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

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import math.permutations.PermutationGenerator;
import math.permutations.PermutationGeneratorProvider;

import org.sql.tablecache.api.index.BroadPrimaryKeyTableIndexer;
import org.sql.tablecache.api.table.TableAccessor;
import org.sql.tablecache.api.table.TableRow;
import org.sql.tablecache.implementation.cache.TableCacheImpl.CacheInfo;

public class BroadPrimaryKeyTableIndexerImpl extends AbstractTableIndexer
    implements BroadPrimaryKeyTableIndexer
{
    private static class TableAccessorImpl
        implements TableAccessor
    {
        static final TableAccessor EMPTY = new TableAccessor()
        {

            @Override
            public Iterator<TableRow> iterator()
            {
                return Collections.EMPTY_SET.iterator();
            }
        };

        private final CacheInfo _cacheInfo;
        private final Map<Object, Object> _index;
        private final int _decidedPKs;
        private final int _maxPKs;

        public TableAccessorImpl( CacheInfo cacheInfo, Map<Object, Object> pkIndex, int decidedPKs, int maxPKs )
        {
            this._cacheInfo = cacheInfo;
            this._index = pkIndex;
            this._decidedPKs = decidedPKs;
            this._maxPKs = maxPKs;
        }

        @Override
        public Iterator<TableRow> iterator()
        {
            return new Iterator<TableRow>()
            {
                private final Deque<Iterator<Object>> _iters = new ArrayDeque<Iterator<Object>>();
                private final int _dequeDepth = _maxPKs - _decidedPKs;

                {
                    if( _index == null || _index.isEmpty() )
                    {
                        this._iters.push( Collections.EMPTY_MAP.values().iterator() );
                    }
                    else
                    {
                        this._iters.push( _index.values().iterator() );
                    }
                    Lock lock = _cacheInfo.getAccessLock().readLock();
                    lock.lock();
                    try
                    {
                        this.reset();
                    }
                    finally
                    {
                        lock.unlock();
                    }
                }

                @Override
                public boolean hasNext()
                {
                    boolean result = false;
                    Lock lock = _cacheInfo.getAccessLock().readLock();
                    lock.lock();
                    try
                    {
                        while( !result && !this._iters.isEmpty() )
                        {
                            result = this._iters.peek().hasNext();
                            if( !result )
                            {
                                this._iters.pop();
                            }
                        }

                        if( result && this._iters.size() < this._dequeDepth )
                        {
                            this.reset();
                        }
                    }
                    finally
                    {
                        lock.unlock();
                    }
                    return result;
                }

                @Override
                public TableRow next()
                {
                    Lock lock = _cacheInfo.getAccessLock().readLock();
                    lock.lock();
                    try
                    {
                        return (TableRow) this._iters.peek().next();
                    }
                    finally
                    {
                        lock.unlock();
                    }
                }

                @Override
                public void remove()
                {
                    throw new UnsupportedOperationException( "Can not remove rows from cache." );
                }

                private void reset()
                {
                    while( this._iters.size() < this._dequeDepth )
                    {
                        Iterator<Object> iter = this._iters.peek();
                        if( iter.hasNext() )
                        {
                            iter = ((Map<Object, Object>) ((Map<String, Object>) iter.next()).values().iterator()
                                .next()).values().iterator();
                        }
                        else
                        {
                            iter = Collections.EMPTY_MAP.values().iterator();
                        }
                        this._iters.push( iter );
                    }
                }
            };
        }
    }

    private final Map<String, Map<Object, Object>> _contents;
    private final CacheInfo _cacheInfo;
    private final PermutationGenerator<String[]> _permutations;
    private final Set<String> _indexingColumnNames;

    public BroadPrimaryKeyTableIndexerImpl( CacheInfo cacheInfo, Set<String> columnNames )
    {
        this._cacheInfo = cacheInfo;
        this._contents = new HashMap<String, Map<Object, Object>>();
        this._indexingColumnNames = columnNames == null ? Collections.unmodifiableSet( cacheInfo.getTableInfo()
            .getPkColumns() ) : Collections.unmodifiableSet( new HashSet<String>( columnNames ) );
        this._permutations = PermutationGeneratorProvider.createGenericComparablePermutationGenerator( String.class,
            this._indexingColumnNames );
    }

    public Set<String> getIndexingColumnNames()
    {
        return this._indexingColumnNames;
    }

    @Override
    public TableRow getRow( String[] indexingColumnNames, Object[] indexingColumnValues )
    {
        Lock lock = this._cacheInfo.getAccessLock().readLock();
        lock.lock();
        try
        {
            Map<Object, Object> current = this._contents.get( indexingColumnNames[0] );
            for( int idx = 1; idx < indexingColumnNames.length; ++idx )
            {
                current = ((Map<String, Map<Object, Object>>) current.get( indexingColumnValues[idx - 1] ))
                    .get( indexingColumnNames[idx] );
            }

            return (TableRow) current.get( indexingColumnValues[indexingColumnValues.length - 1] );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public TableRow getRow( Object pk )
    {
        Lock lock = this._cacheInfo.getAccessLock().readLock();
        lock.lock();
        try
        {
            return (TableRow) this._contents.values().iterator().next().get( pk );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public Boolean hasRow( Object pk )
    {
        Lock lock = this._cacheInfo.getAccessLock().readLock();
        lock.lock();
        try
        {
            return this._contents.values().iterator().next().containsKey( pk );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void insertOrUpdateRow( TableRow newRow )
    {
        // Write-locking is not required, as table cache should do it.
        for( String[] pkNames : this._permutations )
        {
            Map<Object, Object> map = (Map) this._contents;
            for( int idx = 0; idx < pkNames.length - 1; ++idx )
            {
                String pkName = pkNames[idx];
                Map<Object, Object> o = (Map<Object, Object>) map.get( pkName );
                if( o == null )
                {
                    o = new HashMap<Object, Object>();
                    map.put( pkName, o );
                }

                Object value = newRow.get( pkName );
                map = (Map<Object, Object>) o.get( value );
                if( map == null )
                {
                    map = new HashMap<Object, Object>();
                    o.put( value, map );
                }
            }

            String pkName = pkNames[pkNames.length - 1];
            Map<Object, Object> o = (Map<Object, Object>) map.get( pkName );
            if( o == null )
            {
                o = new HashMap<Object, Object>();
                map.put( pkName, o );
            }
            o.put( newRow.get( pkName ), newRow );
        }
    }

    @Override
    public TableAccessor getRows()
    {
        // TODO use Iterables.flattenIterables
        return new TableAccessorImpl( this._cacheInfo, this._contents.values().iterator().next(), 0,
            this._indexingColumnNames.size() );
    }

    @Override
    public TableAccessor getRowsPartialPK( String[] indexingColumnNames, Object[] indexingColumnValues )
    {
        // TODO use Iterables.flattenIterables
        Lock lock = this._cacheInfo.getAccessLock().readLock();
        lock.lock();
        try
        {
            Map<Object, Object> current = this._contents.get( indexingColumnNames[0] );
            for( int idx = 1; idx < indexingColumnNames.length; ++idx )
            {
                Map<String, Map<Object, Object>> mapz = (Map<String, Map<Object, Object>>) current
                    .get( indexingColumnValues[idx - 1] );
                if( mapz != null )
                {
                    current = mapz.get( indexingColumnNames[idx] );
                }
                else
                {
                    break;
                }
            }

            TableAccessor result = null;
            if( current != null )
            {
                Map<Object, Object> mapz = (Map<Object, Object>) current
                    .get( indexingColumnValues[indexingColumnValues.length - 1] );
                if( mapz == null )
                {
                    result = TableAccessorImpl.EMPTY;
                }
                else
                {
                    result = new TableAccessorImpl( this._cacheInfo, (Map<Object, Object>) (mapz).values().iterator()
                        .next(), indexingColumnNames.length, this._indexingColumnNames.size() );
                }
            }
            else
            {
                result = TableAccessorImpl.EMPTY;
            }

            return result;
        }
        finally
        {
            lock.unlock();
        }
    }
}