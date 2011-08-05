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
import java.util.HashSet;
import java.util.Set;

public class JDBCMetaDataPrimaryKeyProvider
    implements PrimaryKeyInfoProvider
{
    @Override
    public PrimaryKeyInfo getPrimaryKeys( Connection connection, final String schemaName, final String tableName )
        throws SQLException
    {
        final Set<String> pks = new HashSet<String>();
        ResultSet cols = connection.getMetaData().getPrimaryKeys( null, schemaName, tableName );
        try
        {
            while( cols.next() )
            {
                pks.add( cols.getString( "COLUMN_NAME" ).toLowerCase() );
            }
        }
        finally
        {
            cols.close();
        }

        return new PrimaryKeyInfo()
        {
            @Override
            public Set<String> getKeyNames()
            {
                return pks;
            }

            @Override
            public Object createThinIndexingMultiKey( TableInfo tableInfo, TableRow row )
            {
                return JDBCMetaDataPrimaryKeyProvider.this.createThinIndexingMultiKey( tableInfo, row );
            }

            @Override
            public Boolean useBroadIndexing()
            {
                return JDBCMetaDataPrimaryKeyProvider.this.useBroadIndexing( schemaName, tableName );
            }

            @Override
            public Boolean useThinIndexing()
            {
                return JDBCMetaDataPrimaryKeyProvider.this.useThinIndexing( schemaName, tableName );
            }
        };
    }

    protected Boolean useBroadIndexing( String schemaName, String tableName )
    {
        return true;
    }

    protected Object createThinIndexingMultiKey( TableInfo tableInfo, TableRow row )
    {
        return null;
    }

    protected Boolean useThinIndexing( String schemaName, String tableName )
    {
        return false;
    }
}