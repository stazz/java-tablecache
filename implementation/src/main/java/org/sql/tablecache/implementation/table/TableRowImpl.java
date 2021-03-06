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

import java.util.Arrays;

import org.sql.tablecache.api.table.NoSuchNamedColumnException;
import org.sql.tablecache.api.table.TableInfo;
import org.sql.tablecache.api.table.TableRow;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public class TableRowImpl
    implements TableRow
{

    private final Object[] _row;
    private final TableInfo _tableInfo;

    public TableRowImpl( TableInfo tableInfo, Object[] row )
    {
        this._tableInfo = tableInfo;
        this._row = row;
    }

    @Override
    public Object get( int index )
    {
        return this._row[index];
    }

    @Override
    public Object get( String columnName )
    {
        try
        {
            return this.get( this._tableInfo.getColumnIndices().get( columnName ) );
        }
        catch( NullPointerException npe )
        {
            throw new NoSuchNamedColumnException( this._tableInfo, columnName );
        }
    }

    @Override
    public <T> T get( Class<T> clazz, int index )
    {
        return clazz.cast( this.get( index ) );
    }

    @Override
    public <T> T get( Class<T> clazz, String columnName )
    {
        return clazz.cast( this.get( columnName ) );
    }

    @Override
    public TableInfo getTableInfo()
    {
        return this._tableInfo;
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null
            && (this == obj || (obj instanceof TableRowImpl && Arrays.equals( this._row, ((TableRowImpl) obj)._row )));
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( this._row );
    }

    @Override
    public String toString()
    {
        return (this._tableInfo.getSchemaName() == null ? "" : this._tableInfo.getSchemaName() + ".")
            + this._tableInfo.getTableName() + Arrays.toString( this._row );
    }

}
