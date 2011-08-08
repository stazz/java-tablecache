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

package org.sql.tablecache.api.table;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public class NoSuchNamedColumnException extends RuntimeException
{

    private final TableInfo _tableInfo;
    private final String _columnName;

    public NoSuchNamedColumnException( TableInfo tableInfo, String columnName )
    {
        super( "No such column " + columnName + " in table "
            + (tableInfo.getSchemaName() == null ? "" : tableInfo.getSchemaName() + ".") + tableInfo.getTableName()
            + "." );
        this._tableInfo = tableInfo;
        this._columnName = columnName;
    }

    public String getColumnName()
    {
        return this._columnName;
    }

    public TableInfo getTableInfo()
    {
        return this._tableInfo;
    }
}
