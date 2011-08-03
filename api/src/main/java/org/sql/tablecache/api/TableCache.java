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

package org.sql.tablecache.api;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.locks.ReadWriteLock;

import org.qi4j.api.common.Optional;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public interface TableCache
{

    public TableInfo getTableInfo( String tableName );

    public TableInfo getTableInfo( String schemaName, String tableName );

    public ReadWriteLock getTableLock( String tableName );

    public ReadWriteLock getTableLock( String schemaName, String tableName );

    public TableIndexer getIndexer( String tableName );

    public TableIndexer getIndexer( String schemaName, String tableName );

    public <AccessorType extends TableIndexer> AccessorType getIndexer( Class<AccessorType> accessorClass,
        String tableName );

    public <AccessorType extends TableIndexer> AccessorType getIndexer( Class<AccessorType> accessorClass,
        String schemaName, String tableName );

    public void buildCache( Connection connection, String schemaName )
        throws SQLException;

    public void buildCache( Connection connection, String schemaName, @Optional TableFilter tableFilter )
        throws SQLException;

    public void buildCache( Connection connection, String schemaName, @Optional TableFilter tableFilter,
        PrimaryKeyInfoProvider detector )
        throws SQLException;

    public void buildCache( Connection connection, String schemaName, String... tableNames )
        throws SQLException;

    public void buildCache( Connection connection, String schemaName, PrimaryKeyInfoProvider detector,
        String... tableNames )
        throws SQLException;

    public Object[] createRow( ResultSet row, TableInfo tableInfo )
        throws SQLException;

    public void clearCache();

    public void insertOrUpdateRow( String tableName, Object[] row );

    public void insertOrUpdateRow( String schemaName, String tableName, Object[] row );

    public interface TableFilter
    {
        public Boolean includeTable( String schemaName, String tableName );
    }
}
