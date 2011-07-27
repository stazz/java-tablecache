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

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import math.permutations.PermutationGenerator;

import org.sql.tablecache.api.TableAccessor;
import org.sql.tablecache.api.TableIndexer.BroadTableIndexer;
import org.sql.tablecache.api.TableInfo;

public class BroadTableCacheAccessorImpl
    implements BroadTableIndexer
{
    private static class TableAccessorImpl
        implements TableAccessor
    {
        static final TableAccessor EMPTY = new TableAccessor()
        {

            @Override
            public Iterator<Object[]> iterator()
            {
                return Collections.EMPTY_SET.iterator();
            }
        };

        private final Map<Object, Object> _pkIndex;
        private final int _decidedPKs;
        private final int _maxPKs;

        public TableAccessorImpl( Map<Object, Object> pkIndex, int decidedPKs, int maxPKs )
        {
            this._pkIndex = pkIndex;
            this._decidedPKs = decidedPKs;
            this._maxPKs = maxPKs;
        }

        @Override
        public Iterator<Object[]> iterator()
        {
            return new Iterator<Object[]>()
            {
                private final Deque<Iterator<Object>> _iters = new ArrayDeque<Iterator<Object>>();
                private final int _dequeDepth = _maxPKs - _decidedPKs;

                {
                    if( _pkIndex == null || _pkIndex.isEmpty() )
                    {
                        this._iters.push( Collections.EMPTY_MAP.values().iterator() );
                    }
                    else
                    {
                        this._iters.push( _pkIndex.values().iterator() );
                    }
                    this.reset();
                }

                @Override
                public boolean hasNext()
                {
                    boolean result = false;
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

                    return result;
                }

                @Override
                public Object[] next()
                {
                    return (Object[]) this._iters.peek().next();
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
    private final TableInfo _tableInfo;

    public BroadTableCacheAccessorImpl( TableInfo tableInfo )
    {
        this._tableInfo = tableInfo;
        this._contents = new HashMap<String, Map<Object, Object>>();
    }

    protected void processRowWithBroadIndexing( Map<String, Integer> columnIndices, Object[] row,
        PermutationGenerator<String[]> permutations )
    {
        for( String[] pkNames : permutations )
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

                Object value = row[columnIndices.get( pkName )];
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
            o.put( row[columnIndices.get( pkName )], row );
        }
    }

    @Override
    public Object[] getRow( String[] pkNames, Object[] pkValues )
    {
        Map<Object, Object> current = this._contents.get( pkNames[0] );
        for( int idx = 1; idx < pkNames.length; ++idx )
        {
            current = ((Map<String, Map<Object, Object>>) current.get( pkValues[idx - 1] )).get( pkNames[idx] );
        }

        return (Object[]) current.get( pkValues[pkValues.length - 1] );
    }

    @Override
    public Object[] getRow( Object pk )
    {
        return (Object[]) this._contents.values().iterator().next().get( pk );
    }

    @Override
    public TableAccessor getRows()
    {
        return new TableAccessorImpl( this._contents.values().iterator().next(), 0, this._tableInfo.getPkColumns()
            .size() );
    }

    @Override
    public TableAccessor getRowsPartialPK( String[] pkNames, Object[] pkValues )
    {
        Set<String> pkNamesSet = this._tableInfo.getPkColumns();
        Map<Object, Object> current = this._contents.get( pkNames[0] );
        for( int idx = 1; idx < pkNames.length; ++idx )
        {
            Map<String, Map<Object, Object>> mapz = (Map<String, Map<Object, Object>>) current.get( pkValues[idx - 1] );
            if( mapz != null )
            {
                current = mapz.get( pkNames[idx] );
            }
            else
            {
                break;
            }
        }

        TableAccessor result = null;
        if( current != null )
        {
            Map<Object, Object> mapz = (Map<Object, Object>) current.get( pkValues[pkValues.length - 1] );
            if( mapz == null )
            {
                result = TableAccessorImpl.EMPTY;
            }
            else
            {
                result = new TableAccessorImpl( (Map<Object, Object>) (mapz).values().iterator().next(),
                    pkNames.length, pkNamesSet.size() );
            }
        }
        else
        {
            result = TableAccessorImpl.EMPTY;
        }
        return result;
    }
}