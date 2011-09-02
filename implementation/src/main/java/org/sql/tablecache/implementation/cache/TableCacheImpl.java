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
import org.sql.tablecache.api.table.Table;
import org.sql.tablecache.api.table.TableInfo;
import org.sql.tablecache.api.table.TableRow;
import org.sql.tablecache.implementation.index.AbstractTableIndexer;
import org.sql.tablecache.implementation.index.BroadUniqueTableIndexerImpl;
import org.sql.tablecache.implementation.index.ThinTableIndexerImpl;
import org.sql.tablecache.implementation.index.ThinUniqueTableIndexerImpl;
import org.sql.tablecache.implementation.table.TableImpl;
import org.sql.tablecache.implementation.table.TableInfoImpl;

public class TableCacheImpl
    implements TableCache
{

    public static class CacheInfo
    {
        private final TableInfo _tableInfo;
        private final Map<String, TableIndexer> _indexers;

        private CacheInfo( TableInfo tableInfo )
        {
            this._tableInfo = tableInfo;
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
    }

    private static final Logger LOGGER = LoggerFactory.getLogger( TableCacheImpl.class );

    private final Map<String, Map<String, Table>> _cacheInfos;

    private final Object _cacheLoadingLock;

    @Service
    private SQLVendorService _vendor;

    public TableCacheImpl()
    {
        this._cacheLoadingLock = new Object();
        this._cacheInfos = new HashMap<String, Map<String, Table>>();
    }

    @Override
    public Table getTable( String schemaName, String tableName )
    {
        synchronized( this._cacheLoadingLock )
        {
            return this._cacheInfos.get( schemaName ).get( tableName );
        }
    }

    @Override
    public Table getTable( String tableName )
    {
        synchronized( this._cacheLoadingLock )
        {
            return this._cacheInfos.values().iterator().next().get( tableName );
        }
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
        Set<TableInfo> tableInfos = new HashSet<TableInfo>();

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
            tableInfos.add( new TableInfoImpl( schemaName, tableName, columnList, pks ) );
        }

        Map<String, Map<String, Table>> tables = new HashMap<String, Map<String, Table>>();
        this.loadContents( connection, tableInfos, tables, indexingInfoProvider );

        synchronized( this._cacheLoadingLock )
        {
            for( Map.Entry<String, Map<String, Table>> entry : tables.entrySet() )
            {
                String schema = entry.getKey();
                Map<String, Table> schemaTables = entry.getValue();
                Map<String, Table> existing = this._cacheInfos.get( schema );
                if( existing == null )
                {
                    this._cacheInfos.put( schemaName, schemaTables );
                }
                else
                {
                    for( Map.Entry<String, Table> tEntry : schemaTables.entrySet() )
                    {
                        // TODO smarter merge.
                        existing.put( tEntry.getKey(), tEntry.getValue() );
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

    protected void loadContents( Connection connection, Set<TableInfo> tableInfos,
        Map<String, Map<String, Table>> tables, IndexingInfoProvider indexingInfo )
        throws SQLException
    {
        Statement stmt = connection.createStatement();
        try
        {
            for( TableInfo tableInfo : tableInfos )
            {
                List<String> cols = tableInfo.getColumns();
                Map<String, IndexingInfo> indexInfo = indexingInfo.getIndexingInfo( tableInfo );
                Map<String, TableIndexer> tableIndexers = new HashMap<String, TableIndexer>();

                for( Map.Entry<String, IndexingInfo> indexInfoEntry : indexInfo.entrySet() )
                {
                    String indexName = indexInfoEntry.getKey();
                    IndexingInfo idxInfo = indexInfoEntry.getValue();
                    switch( idxInfo.getIndexType() ) {
                    case BROAD_PK:
                        Set<String> idxCols = ((BroadPrimaryKeyIndexingInfo) idxInfo).getIndexingColumns();
                        tableIndexers.put( indexName,
                            new BroadUniqueTableIndexerImpl( idxCols == null ? tableInfo.getPkColumns() : idxCols ) );
                        break;
                    case THIN_PK:
                        tableIndexers.put( indexName, new ThinUniqueTableIndexerImpl(
                            ((ThinPrimaryKeyIndexingInfo) idxInfo).getPkProvider() ) );
                        break;
                    case THIN:
                        tableIndexers.put( indexName,
                            new ThinTableIndexerImpl( ((ThinIndexingInfo) idxInfo).getKeyProvider() ) );
                        break;
                    default:
                        throw new IllegalArgumentException( "Unknown indexer type: " + idxInfo.getIndexType() );
                    }
                }

                ResultSet rs = stmt.executeQuery( this.getQueryForAllRows( tableInfo.getSchemaName(),
                    tableInfo.getTableName(), cols ) );

                Table table = new TableImpl( tableInfo, tableIndexers );
                Map<String, Table> tablez = tables.get( tableInfo.getSchemaName() );
                if( tablez == null )
                {
                    tablez = new HashMap<String, Table>();
                    tables.put( tableInfo.getSchemaName(), tablez );
                }
                tablez.put( tableInfo.getTableName(), table );

                try
                {
                    while( rs.next() )
                    {
                        TableRow row = table.createRow( rs );
                        for( Map.Entry<String, TableIndexer> iEntry : tableIndexers.entrySet() )
                        {
                            ((AbstractTableIndexer) iEntry.getValue()).insertOrUpdateRow( row );
                        }
                    }
                    LOGGER.info( "Successfully loaded table " + tableInfo + " with indexers " + tableIndexers.keySet()
                        + "." );
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