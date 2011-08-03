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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import math.permutations.PermutationGenerator;
import math.permutations.PermutationGeneratorProvider;

import org.qi4j.api.injection.scope.Service;
import org.sql.generation.api.qi4j.SQLVendorService;
import org.sql.generation.api.vendor.SQLVendor;
import org.sql.tablecache.api.JDBCMetaDataPrimaryKeyProvider;
import org.sql.tablecache.api.PrimaryKeyInfoProvider;
import org.sql.tablecache.api.PrimaryKeyInfoProvider.PrimaryKeyInfo;
import org.sql.tablecache.api.TableCache;
import org.sql.tablecache.api.TableIndexer;
import org.sql.tablecache.api.TableIndexer.BroadTableIndexer;
import org.sql.tablecache.api.TableIndexer.ThinTableIndexer;
import org.sql.tablecache.api.TableInfo;

public class TableCacheImpl
    implements TableCache
{

    protected static class CacheInfo
    {
        private final TableInfo _tableInfo;
        private final Map<Class<?>, TableIndexer> _accessors;
        private final ReadWriteLock _accessLock;

        private CacheInfo( TableInfo tableInfo )
        {
            this._tableInfo = tableInfo;
            this._accessors = new HashMap<Class<?>, TableIndexer>();
            this._accessLock = new ReentrantReadWriteLock();
        }

        public Map<Class<?>, TableIndexer> getAccessors()
        {
            return this._accessors;
        }

        public TableInfo getTableInfo()
        {
            return this._tableInfo;
        }

        public ReadWriteLock getAccessLock()
        {
            return this._accessLock;
        }
    }

    private final Map<String, Map<String, CacheInfo>> _cacheInfos;

    private final Object _cacheLoadingLock;

    @Service
    private SQLVendorService _vendor;

    public TableCacheImpl()
    {
        this._cacheLoadingLock = new Object();
        this._cacheInfos = new HashMap<String, Map<String, CacheInfo>>();
    }

    protected CacheInfo getCacheInfo( String tableName )
    {
        synchronized( this._cacheLoadingLock )
        {
            return this._cacheInfos.values().iterator().next().get( tableName );
        }
    }

    protected CacheInfo getCacheInfo( String schemaName, String tableName )
    {
        synchronized( this._cacheLoadingLock )
        {
            return this._cacheInfos.get( schemaName ).get( tableName );
        }
    }

    @Override
    public TableInfo getTableInfo( String schemaName, String tableName )
    {
        return this.getCacheInfo( schemaName, tableName ).getTableInfo();
    }

    @Override
    public TableInfo getTableInfo( String tableName )
    {
        return this.getCacheInfo( tableName ).getTableInfo();
    }

    @Override
    public <AccessorType extends TableIndexer> AccessorType getIndexer( Class<AccessorType> accessorClass,
        String schemaName, String tableName )
    {
        return accessorClass.cast( this.getCacheInfo( schemaName, tableName ).getAccessors().get( accessorClass ) );
    }

    @Override
    public <AccessorType extends TableIndexer> AccessorType getIndexer( Class<AccessorType> accessorClass,
        String tableName )
    {
        return accessorClass.cast( this.getCacheInfo( tableName ).getAccessors().get( accessorClass ) );
    }

    @Override
    public TableIndexer getIndexer( String schemaName, String tableName )
    {
        return this.getCacheInfo( schemaName, tableName ).getAccessors().values().iterator().next();
    }

    @Override
    public TableIndexer getIndexer( String tableName )
    {
        return this.getCacheInfo( tableName ).getAccessors().values().iterator().next();
    }

    @Override
    public ReadWriteLock getTableLock( String schemaName, String tableName )
    {
        return this.getCacheInfo( schemaName, tableName ).getAccessLock();
    }

    @Override
    public ReadWriteLock getTableLock( String tableName )
    {
        return this.getCacheInfo( tableName ).getAccessLock();
    }

    @Override
    public void insertOrUpdateRow( String schemaName, String tableName, Object[] row )
    {
        this.doInsertOrUpdateRow( this.getCacheInfo( schemaName, tableName ), row );
    }

    @Override
    public void insertOrUpdateRow( String tableName, Object[] row )
    {
        this.doInsertOrUpdateRow( this.getCacheInfo( tableName ), row );
    }

    protected void doInsertOrUpdateRow( CacheInfo cacheInfo, Object[] row )
    {
        Lock lock = cacheInfo.getAccessLock().writeLock();
        lock.lock();
        try
        {
            for( TableIndexer indexer : cacheInfo.getAccessors().values() )
            {
                ((AbstractTableIndexer) indexer).insertOrUpdateRow( row );
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.trinity.db.runtime.metameta.TableCachingService#buildCache()
     */
    @Override
    public void buildCache( Connection connection, String schemaName )
        throws SQLException
    {
        this.buildCache( connection, schemaName, (TableFilter) null );
    }

    @Override
    public void buildCache( Connection connection, String schemaName, TableFilter tableFilter )
        throws SQLException
    {
        this.buildCache( connection, schemaName, tableFilter, new JDBCMetaDataPrimaryKeyProvider() );
    }

    @Override
    public void buildCache( Connection connection, String schemaName, TableFilter tableFilter,
        PrimaryKeyInfoProvider detector )
        throws SQLException
    {
        List<String> tables = new ArrayList<String>();

        ResultSet rs = connection.getMetaData().getTables( null, schemaName, null, new String[]
        {
            "TABLE"
        } );
        try
        {
            while( rs.next() )
            {
                String tableName = rs.getString( "TABLE_NAME" );
                if( tableFilter == null || tableFilter.includeTable( schemaName, tableName ) )
                {
                    tables.add( tableName );
                }
            }
        }
        finally
        {
            rs.close();
        }

        this.buildCache( connection, schemaName, detector, tables.toArray( new String[tables.size()] ) );
    }

    @Override
    public void buildCache( Connection connection, String schemaName, String... tableNames )
        throws SQLException
    {
        this.buildCache( connection, schemaName, new JDBCMetaDataPrimaryKeyProvider(), tableNames );
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.trinity.db.runtime.metameta.TableCachingService#buildCache(java.sql.Connection, java.lang.String,
     * org.trinity.db.runtime.metameta.TableCachingService.PrimaryKeyDetector, java.lang.String[])
     */
    @Override
    public void buildCache( Connection connection, String schemaName, PrimaryKeyInfoProvider detector,
        String... tableNames )
        throws SQLException
    {
        Map<String, CacheInfo> map = new HashMap<String, CacheInfo>();

        for( String tableName : tableNames )
        {
            List<String> columnList = new ArrayList<String>();
            ResultSet cols = connection.getMetaData().getColumns( null, schemaName, tableName, null );
            try
            {
                while( cols.next() )
                {
                    columnList.add( cols.getString( "COLUMN_NAME" ).toLowerCase() );
                }
            }
            finally
            {
                cols.close();
            }
            PrimaryKeyInfo pkInfo = detector.getPrimaryKeys( connection, schemaName, tableName );

            map.put( tableName, new CacheInfo( new TableInfoImpl( tableName, columnList, pkInfo ) ) );
        }

        this.loadContents( connection, schemaName, map );

        synchronized( this._cacheLoadingLock )
        {
            Map<String, CacheInfo> existing = this._cacheInfos.get( schemaName );
            if( existing == null )
            {
                this._cacheInfos.put( schemaName, map );
            }
            else
            {
                for( Map.Entry<String, CacheInfo> entry : map.entrySet() )
                {
                    if( existing.containsKey( entry.getKey() ) )
                    {
                        // TODO smarter merge.
                        synchronized( existing.get( entry.getKey() ).getAccessLock() )
                        {
                            existing.put( entry.getKey(), entry.getValue() );
                        }
                    }
                    else
                    {
                        existing.put( entry.getKey(), entry.getValue() );
                    }
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.trinity.db.runtime.metameta.TableCachingService#clearCache()
     */
    @Override
    public void clearCache()
    {
        synchronized( this._cacheLoadingLock )
        {
            this._cacheInfos.clear();
        }
    }

    @Override
    public Object[] createRow( ResultSet row, TableInfo tableInfo )
        throws SQLException
    {
        List<String> cols = tableInfo.getColumns();
        Object[] result = new Object[cols.size()];
        // Populate row array in a order as specified by column name list 
        for( String col : cols )
        {
            result[cols.indexOf( col )] = row.getObject( col );
        }

        return result;
    }

    protected void loadContents( Connection connection, String schemaName, Map<String, CacheInfo> cacheInfos )
        throws SQLException
    {
        Statement stmt = connection.createStatement();
        try
        {
            for( String table : cacheInfos.keySet() )
            {
                CacheInfo cacheInfo = cacheInfos.get( table );
                TableInfo tableInfo = cacheInfo.getTableInfo();
                List<String> cols = tableInfo.getColumns();
                Map<Class<?>, TableIndexer> tableAccessors = cacheInfo.getAccessors();

                Boolean useBroadIndexing = tableInfo.useBroadIndexing();
                Boolean useThinIndexing = tableInfo.useThinIndexing();

                // For broad indexing
                BroadTableCacheAccessorImpl broadCache = null;
                if( useBroadIndexing )
                {
                    broadCache = new BroadTableCacheAccessorImpl( cacheInfo );
                    tableAccessors.put( BroadTableIndexer.class, broadCache );
                }

                // For thin indexing
                ThinTableCacheAccessorImpl thinCache = null;
                if( useThinIndexing )
                {
                    thinCache = new ThinTableCacheAccessorImpl( cacheInfo );
                    tableAccessors.put( ThinTableIndexer.class, thinCache );
                }

                ResultSet rs = stmt.executeQuery( this.getQueryForAllRows( schemaName, table, cols ) );
                try
                {
                    while( rs.next() )
                    {
                        Object[] row = this.createRow( rs, tableInfo );

                        // Further process the row (add it to cache)
                        if( useBroadIndexing )
                        {
                            broadCache.insertOrUpdateRow( row );
                        }
                        if( useThinIndexing )
                        {
                            thinCache.insertOrUpdateRow( row );
                        }
                    }
                }
                finally
                {
                    rs.close();
                }
            }
        }
        finally
        {
            stmt.close();
        }
    }

    protected String getQueryForAllRows( String schema, String table, Collection<String> colNames )
    {
        SQLVendor vendor = this._vendor.getSQLVendor();

        // @formatter:off
        return vendor.toString(
            vendor.getQueryFactory().simpleQueryBuilder()
             .select( colNames.toArray( new String[colNames.size()] ) )
             .from( vendor.getTableReferenceFactory().tableName( schema, table ) )
             .createExpression()
             );
        // @formatter:on
    }

}