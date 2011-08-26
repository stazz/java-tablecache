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

package org.sql.tablecache.implementation.cache;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.qi4j.api.injection.scope.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql.generation.api.qi4j.SQLVendorService;
import org.sql.generation.api.vendor.SQLVendor;
import org.sql.tablecache.api.cache.TableCache;
import org.sql.tablecache.api.callbacks.IndexingInfoProvider;
import org.sql.tablecache.api.callbacks.IndexingInfoProvider.DefaultIndexingInfoProvider;
import org.sql.tablecache.api.callbacks.PrimaryKeyOverride;
import org.sql.tablecache.api.index.IndexingInfo;
import org.sql.tablecache.api.index.IndexingInfo.BroadPrimaryKeyIndexingInfo;
import org.sql.tablecache.api.index.IndexingInfo.ThinIndexingInfo;
import org.sql.tablecache.api.index.IndexingInfo.ThinPrimaryKeyIndexingInfo;
import org.sql.tablecache.api.index.TableIndexer;
import org.sql.tablecache.api.table.TableInfo;
import org.sql.tablecache.api.table.TableRow;
import org.sql.tablecache.implementation.index.AbstractTableIndexer;
import org.sql.tablecache.implementation.index.BroadPrimaryKeyTableIndexerImpl;
import org.sql.tablecache.implementation.index.ThinPrimaryKeyTableIndexerImpl;
import org.sql.tablecache.implementation.index.ThinTableIndexerImpl;
import org.sql.tablecache.implementation.table.TableInfoImpl;
import org.sql.tablecache.implementation.table.TableRowImpl;

public class TableCacheImpl
    implements TableCache
{

    public static class CacheInfo
    {
        private final TableInfo _tableInfo;
        private final Map<String, TableIndexer> _indexers;
        private final ReadWriteLock _accessLock;

        private CacheInfo( TableInfo tableInfo )
        {
            this._tableInfo = tableInfo;
            this._accessLock = new ReentrantReadWriteLock();
            this._indexers = new HashMap<String, TableIndexer>();
        }

        public Map<String, TableIndexer> getIndexers()
        {
            return this._indexers;
        }

        public TableInfo getTableInfo()
        {
            return this._tableInfo;
        }

        /**
         * TODO this might be useless? Since atm in the project using this, the synchronization is done outside. We
         * could just say that this is very simple table cache and syncing should be done from the outside.
         * 
         * @return
         */
        public ReadWriteLock getAccessLock()
        {
            return this._accessLock;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger( TableCacheImpl.class );

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
        CacheInfo info = this.getCacheInfo( schemaName, tableName );
        if( info == null )
        {
            throw new IllegalArgumentException( "No such table " + (schemaName == null ? "" : schemaName + ".")
                + tableName + "." );
        }
        return info.getTableInfo();
    }

    @Override
    public TableInfo getTableInfo( String tableName )
    {
        return this.getCacheInfo( tableName ).getTableInfo();
    }

    @Override
    public boolean isTableLoaded( String schemaName, String tableName )
    {
        synchronized( this._cacheLoadingLock )
        {
            return this._cacheInfos.containsKey( schemaName )
                && this._cacheInfos.get( schemaName ).containsKey( tableName );
        }
    }

    @Override
    public boolean isTableLoaded( String tableName )
    {
        synchronized( this._cacheLoadingLock )
        {
            return !this._cacheInfos.isEmpty() && this._cacheInfos.values().iterator().next().containsKey( tableName );
        }
    }

    @Override
    public <AccessorType extends TableIndexer> AccessorType getDefaultIndexer( Class<AccessorType> accessorClass,
        String schemaName, String tableName )
    {
        return accessorClass.cast( this.getCacheInfo( schemaName, tableName ).getIndexers().get( null ) );
    }

    @Override
    public <AccessorType extends TableIndexer> AccessorType getDefaultIndexer( Class<AccessorType> accessorClass,
        String tableName )
    {
        return accessorClass.cast( this.getCacheInfo( tableName ).getIndexers().get( null ) );
    }

    @Override
    public <AccessorType extends TableIndexer> AccessorType getIndexer( Class<AccessorType> accessorClass,
        String schemaName, String tableName, String indexName )
    {
        return accessorClass.cast( this.getCacheInfo( schemaName, tableName ).getIndexers().get( indexName ) );
    }

    @Override
    public <AccessorType extends TableIndexer> AccessorType getIndexer( Class<AccessorType> accessorClass,
        String tableName, String indexName )
    {
        return accessorClass.cast( this.getCacheInfo( tableName ).getIndexers().get( indexName ) );
    }

    @Override
    public TableIndexer getDefaultIndexer( String schemaName, String tableName )
    {
        return this.getCacheInfo( schemaName, tableName ).getIndexers().values().iterator().next();
    }

    @Override
    public TableIndexer getDefaultIndexer( String tableName )
    {
        return this.getCacheInfo( tableName ).getIndexers().values().iterator().next();
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
    public void insertOrUpdateRows( TableRow... rows )
    {
        Map<String, Map<String, List<TableRow>>> sortedRows = new HashMap<String, Map<String, List<TableRow>>>();
        for( TableRow row : rows )
        {
            TableInfo tableInfo = row.getTableInfo();
            String schemaName = tableInfo.getSchemaName();
            String tableName = tableInfo.getTableName();

            Map<String, List<TableRow>> rowz = sortedRows.get( schemaName );
            if( rowz == null )
            {
                rowz = new HashMap<String, List<TableRow>>();
                sortedRows.put( schemaName, rowz );
            }
            List<TableRow> rowzz = rowz.get( tableName );
            if( rowzz == null )
            {
                rowzz = new ArrayList<TableRow>();
                rowz.put( tableName, rowzz );
            }

            rowzz.add( row );
        }

        for( Map.Entry<String, Map<String, List<TableRow>>> entry : sortedRows.entrySet() )
        {
            for( Map.Entry<String, List<TableRow>> entry2 : entry.getValue().entrySet() )
            {
                this.doInsertOrUpdateRows( entry.getKey(), entry2.getKey(), entry2.getValue() );
            }
        }
    }

    protected void doInsertOrUpdateRows( String schemaName, String tableName, List<TableRow> rows )
    {
        CacheInfo cacheInfo = this.getCacheInfo( schemaName, tableName );
        Lock lock = cacheInfo.getAccessLock().writeLock();
        lock.lock();
        try
        {
            for( TableIndexer indexer : cacheInfo.getIndexers().values() )
            {
                for( TableRow row : rows )
                {
                    ((AbstractTableIndexer) indexer).insertOrUpdateRow( row );
                }
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
        this.buildCache( connection, schemaName, tableFilter, DefaultIndexingInfoProvider.INSTANCE );
    }

    @Override
    public void buildCache( Connection connection, String schemaName, TableFilter tableFilter,
        IndexingInfoProvider provider )
        throws SQLException
    {
        this.buildCache( connection, schemaName, tableFilter, provider, (PrimaryKeyOverride) null );
    }

    @Override
    public void buildCache( Connection connection, String schemaName, String... tableNames )
        throws SQLException
    {
        this.buildCache( connection, schemaName, DefaultIndexingInfoProvider.INSTANCE, tableNames );
    }

    @Override
    public void buildCache( Connection connection, String schemaName, IndexingInfoProvider indexingInfo,
        String... tableNames )
        throws SQLException
    {
        this.buildCache( connection, schemaName, indexingInfo, null, tableNames );
    }

    @Override
    public void buildCache( Connection connection, String schemaName, IndexingInfoProvider indexingInfoProvider,
        PrimaryKeyOverride pkOverride, String... tableNames )
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

            Set<String> pks = null;
            if( pkOverride != null )
            {
                pks = pkOverride.getPrimaryKeys( schemaName, tableName );
                if( pks != null && pks.isEmpty() )
                {
                    pks = null;
                }
            }
            if( pks == null )
            {
                pks = new HashSet<String>();
                cols = connection.getMetaData().getPrimaryKeys( null, schemaName, tableName );
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
            }
            map.put( tableName, new CacheInfo( new TableInfoImpl( schemaName, tableName, columnList, pks ) ) );
        }

        this.loadContents( connection, schemaName, map, indexingInfoProvider );

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

    @Override
    public void buildCache( Connection connection, String schemaName, TableFilter tableFilter,
        IndexingInfoProvider indexingInfoProvider, PrimaryKeyOverride pkOverride )
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

        this.buildCache( connection, schemaName, indexingInfoProvider, pkOverride,
            tables.toArray( new String[tables.size()] ) );
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
    public TableRow createRow( ResultSet row, TableInfo tableInfo )
        throws SQLException
    {
        List<String> cols = tableInfo.getColumns();
        Object[] result = new Object[cols.size()];
        // Populate row array in a order as specified by column name list
        int idx = 0;
        for( String col : cols )
        {
            result[idx] = row.getObject( col );
            ++idx;
        }

        return new TableRowImpl( tableInfo, result );
    }

    protected void loadContents( Connection connection, String schemaName, Map<String, CacheInfo> cacheInfos,
        IndexingInfoProvider indexingInfo )
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
                Map<String, IndexingInfo> indexInfo = indexingInfo.getIndexingInfo( tableInfo );
                Map<String, TableIndexer> indexers = cacheInfo.getIndexers();

                for( Map.Entry<String, IndexingInfo> indexInfoEntry : indexInfo.entrySet() )
                {
                    String indexName = indexInfoEntry.getKey();
                    IndexingInfo idxInfo = indexInfoEntry.getValue();
                    switch( idxInfo.getIndexType() ) {
                    case BROAD_PK:
                        indexers.put( indexName, new BroadPrimaryKeyTableIndexerImpl( cacheInfo,
                            ((BroadPrimaryKeyIndexingInfo) idxInfo).getIndexingColumns() ) );
                        break;
                    case THIN_PK:
                        indexers.put( indexName, new ThinPrimaryKeyTableIndexerImpl( cacheInfo,
                            ((ThinPrimaryKeyIndexingInfo) idxInfo).getPkProvider() ) );
                        break;
                    case THIN:
                        indexers.put( indexName,
                            new ThinTableIndexerImpl( cacheInfo, ((ThinIndexingInfo) idxInfo).getKeyProvider() ) );
                        break;
                    default:
                        throw new IllegalArgumentException( "Unknown indexer type: " + idxInfo.getIndexType() );
                    }
                }

                ResultSet rs = stmt.executeQuery( this.getQueryForAllRows( schemaName, table, cols ) );
                try
                {
                    while( rs.next() )
                    {
                        TableRow row = this.createRow( rs, tableInfo );
                        for( Map.Entry<String, TableIndexer> entry : indexers.entrySet() )
                        {
                            ((AbstractTableIndexer) entry.getValue()).insertOrUpdateRow( row );
                        }
                    }
                    LOGGER.info( "Successfully loaded table " + (schemaName == null ? "" : schemaName + ".") + table
                        + " with indexers " + indexers.keySet() + "." );
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