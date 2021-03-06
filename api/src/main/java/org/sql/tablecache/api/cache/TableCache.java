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

package org.sql.tablecache.api.cache;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.qi4j.api.common.Optional;
import org.sql.tablecache.api.callbacks.IndexingInfoProvider;
import org.sql.tablecache.api.callbacks.PrimaryKeyOverride;
import org.sql.tablecache.api.table.Table;
import org.sql.tablecache.api.table.TableInfo;
import org.sql.tablecache.api.table.TableRow;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public interface TableCache
{

    public Table getTable( String tableName );

    public Table getTable( String schemaName, String tableName );

    public boolean isTableLoaded( String tableName );

    public boolean isTableLoaded( String schemaName, String tableName );

    public void buildCache( Connection connection, String schemaName )
        throws SQLException;

    public void buildCache( Connection connection, String schemaName, @Optional TableFilter tableFilter )
        throws SQLException;

    public void buildCache( Connection connection, String schemaName, @Optional TableFilter tableFilter,
        IndexingInfoProvider indexingInfoProvider )
        throws SQLException;

    public void buildCache( Connection connection, String schemaName, @Optional TableFilter tableFilter,
        IndexingInfoProvider indexingInfoProvider, @Optional PrimaryKeyOverride pkOverride )
        throws SQLException;

    public void buildCache( Connection connection, String schemaName, String... tableNames )
        throws SQLException;

    public void buildCache( Connection connection, String schemaName, IndexingInfoProvider indexingInfoProvider,
        String... tableNames )
        throws SQLException;

    public void buildCache( Connection connection, String schemaName, IndexingInfoProvider indexingInfoProvider,
        @Optional PrimaryKeyOverride pkOverride, String... tableNames )
        throws SQLException;

    public void clearCache();

    public interface TableFilter
    {
        public Boolean includeTable( String schemaName, String tableName );
    }
}
