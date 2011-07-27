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

import org.sql.tablecache.api.TableAccessor;
import org.sql.tablecache.api.TableInfo;
import org.sql.tablecache.api.TableIndexer.ThinTableIndexer;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public class ThinTableCacheAccessorImpl
    implements ThinTableIndexer
{

    private static class ThinTableAccessor
        implements TableAccessor
    {

        private final Map<Object, Object[]> _rows;

        private ThinTableAccessor( Map<Object, Object[]> rows )
        {
            this._rows = rows;
        }

        @Override
        public Iterator<Object[]> iterator()
        {
            return this._rows.values().iterator();
        }

    }

    private final Map<Object, Object[]> _rows;
    private final TableInfo _tableInfo;

    public ThinTableCacheAccessorImpl( TableInfo tableInfo )
    {
        this._rows = new HashMap<Object, Object[]>();
        this._tableInfo = tableInfo;
    }

    protected void processRowWithThinIndexing( Object[] row )
    {
        Object pk = this._tableInfo.createThinIndexPK( row );
        this._rows.put( pk, row );
    }

    @Override
    public Object[] getRow( Object pk )
    {
        return this._rows.get( pk );
    }

    @Override
    public TableAccessor getRows()
    {
        return new ThinTableAccessor( this._rows );
    }
}
