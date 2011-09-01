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

import java.util.Iterator;

import org.sql.tablecache.api.table.TableAccessor;
import org.sql.tablecache.api.table.TableRow;
import org.sql.tablecache.implementation.cache.TableCacheImpl.CacheInfo;

class TableAccessorImpl
    implements TableAccessor
{
    private final CacheInfo _cacheInfo;
    private final Iterable<TableRow> _realIterable;

    TableAccessorImpl( CacheInfo cacheInfo, Iterable<TableRow> realIterable )
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
                return _realIterator.hasNext();
            }

            @Override
            public TableRow next()
            {
                return _realIterator.next();
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException( "Indexing is read-only." );
            }
        };
    }

}