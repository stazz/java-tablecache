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
import java.util.Set;

import math.permutations.PermutationGenerator;
import math.permutations.PermutationGeneratorProvider;

import org.qi4j.api.injection.scope.Service;
import org.sql.generation.api.qi4j.SQLVendorService;
import org.sql.generation.api.vendor.SQLVendor;
import org.sql.tablecache.api.PrimaryKeyInfoProvider;
import org.sql.tablecache.api.PrimaryKeyInfoProvider.JDBCMetaDataPrimaryKeyDetector;
import org.sql.tablecache.api.PrimaryKeyInfoProvider.PrimaryKeyInfo;
import org.sql.tablecache.api.TableCache;
import org.sql.tablecache.api.TableIndexer;
import org.sql.tablecache.api.TableIndexer.BroadTableIndexer;
import org.sql.tablecache.api.TableIndexer.ThinTableIndexer;
import org.sql.tablecache.api.TableInfo;

public class TableCacheImpl
    implements TableCache
{

    public static class TableInfoImpl
        implements TableInfo
    {
        private final String _tableName;
        private final List<String> _columns;
        private final Map<String, Integer> _columnIndices;
        private final Map<String, Integer> _pkIndices;
        private final PrimaryKeyInfo _pkInfo;

        public TableInfoImpl( String tableName, List<String> columns, PrimaryKeyInfo pkInfo )
        {
            this._tableName = tableName;
            this._columns = columns;
            this._pkInfo = pkInfo;
            this._columnIndices = new HashMap<String, Integer>();
            Set<String> pkColumns = this._pkInfo.getKeyNames();
            this._pkIndices = new HashMap<String, Integer>( pkColumns.size() );
            Integer pkIdx = 0;
            for( Integer idx = 0; idx < columns.size(); ++idx )
            {
                String col = columns.get( idx );
                this._columnIndices.put( col, idx );
                if( pkColumns.contains( col ) )
                {
                    this._pkIndices.put( col, pkIdx );
                    ++pkIdx;
                }
            }
        }

        @Override
        public Map<String, Integer> getColumnIndices()
        {
            return this._columnIndices;
        }

        @Override
        public Set<String> getPkColumns()
        {
            return this._pkInfo.getKeyNames();
        }

        @Override
        public Map<String, Integer> getPkIndices()
        {
            return this._pkIndices;
        }

        @Override
        public List<String> getColumns()
        {
            return this._columns;
        }

        @Override
        public Object createThinIndexPK( Object[] row )
        {
            return this._pkInfo.createThinIndexingMultiKey( this, row );
        }

        @Override
        public String getTableName()
        {
            return this._tableName;
        }

        @Override
        public Boolean useBroadIndexing()
        {
            return this._pkInfo.useBroadIndexing();
        }

        @Override
        public Boolean useThinIndexing()
        {
            return this._pkInfo.useThinIndexing();
        }
    }

    private Map<String, Map<String, TableInfo>> _tableInfos;

    private Map<String, Map<String, Map<Class<?>, TableIndexer>>> _accessors;

    @Service
    private SQLVendorService _vendor;

    public TableCacheImpl()
    {
        this._tableInfos = new HashMap<String, Map<String, TableInfo>>();
        this._accessors = new HashMap<String, Map<String, Map<Class<?>, TableIndexer>>>();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.trinity.db.runtime.metameta.TableCachingService#getTableInfos()
     */
    @Override
    public Map<String, Map<String, TableInfo>> getTableInfos()
    {
        return this._tableInfos;
    }

    @Override
    public <AccessorType extends TableIndexer> AccessorType getIndexer( Class<AccessorType> accessorClass,
        String schemaName, String tableName )
    {
        return accessorClass.cast( this._accessors.get( schemaName ).get( tableName ).get( accessorClass ) );
    }

    @Override
    public <AccessorType extends TableIndexer> AccessorType getIndexer( Class<AccessorType> accessorClass,
        String tableName )
    {
        return this.getIndexer( accessorClass, this._accessors.keySet().iterator().next(), tableName );
    }

    @Override
    public TableIndexer getIndexer( String schemaName, String tableName )
    {
        return this._accessors.get( schemaName ).get( tableName ).values().iterator().next();
    }

    @Override
    public TableIndexer getIndexer( String tableName )
    {
        return this.getIndexer( this._accessors.keySet().iterator().next(), tableName );
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
        this.buildCache( connection, schemaName, tableFilter, new JDBCMetaDataPrimaryKeyDetector() );
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
        this.buildCache( connection, schemaName, new JDBCMetaDataPrimaryKeyDetector(), tableNames );
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
        Map<String, TableInfo> map = this._tableInfos.get( schemaName );
        if( map == null )
        {
            map = new HashMap<String, TableInfo>();
            this._tableInfos.put( schemaName, map );
        }

        for( String tableName : tableNames )
        {
            List<String> columnList = new ArrayList<String>();
            ResultSet cols = connection.getMetaData().getColumns( null, schemaName, tableName, null );
            try
            {
                while( cols.next() )
                {
                    String colName = cols.getString( "COLUMN_NAME" );
                    columnList.add( colName );
                }

            }
            finally
            {
                cols.close();
            }
            cols = connection.getMetaData().getPrimaryKeys( null, schemaName, tableName );
            PrimaryKeyInfo pkInfo = detector.getPrimaryKeys( connection, schemaName, tableName );

            map.put( tableName, new TableCacheImpl.TableInfoImpl( tableName, columnList, pkInfo ) );
        }

        this.loadContents( connection, schemaName, map );
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.trinity.db.runtime.metameta.TableCachingService#clearCache()
     */
    @Override
    public void clearCache()
    {
        this._tableInfos.clear();
        this._accessors.clear();
    }

    protected void loadContents( Connection connection, String schemaName, Map<String, TableInfo> tableInfos )
        throws SQLException
    {
        Statement stmt = connection.createStatement();
        try
        {
            for( String table : tableInfos.keySet() )
            {
                TableInfo tableInfo = tableInfos.get( table );
                Map<String, Integer> colIndices = tableInfo.getColumnIndices();
                Map<String, Map<Class<?>, TableIndexer>> schemaAccessors = this._accessors.get( schemaName );
                if( schemaAccessors == null )
                {
                    schemaAccessors = new HashMap<String, Map<Class<?>, TableIndexer>>();
                    this._accessors.put( schemaName, schemaAccessors );
                }
                Map<Class<?>, TableIndexer> tableAccessors = schemaAccessors.get( table );
                if( tableAccessors == null )
                {
                    tableAccessors = new HashMap<Class<?>, TableIndexer>();
                    schemaAccessors.put( table, tableAccessors );
                }

                // For broad indexing
                PermutationGenerator<String[]> permutation = null;
                BroadTableCacheAccessorImpl broadCache = null;
                if( tableInfo.useBroadIndexing() )
                {
                    permutation = PermutationGeneratorProvider.createGenericComparablePermutationGenerator(
                        String.class, tableInfo.getPkColumns() );
                    broadCache = new BroadTableCacheAccessorImpl( tableInfo );
                    tableAccessors.put( BroadTableIndexer.class, broadCache );
                }

                // For thin indexing
                ThinTableCacheAccessorImpl thinCache = null;
                if( tableInfo.useThinIndexing() )
                {
                    thinCache = new ThinTableCacheAccessorImpl( tableInfo );
                    tableAccessors.put( ThinTableIndexer.class, thinCache );
                }

                ResultSet rs = stmt.executeQuery( this.getQueryForAllRows( schemaName, table, colIndices.keySet() ) );
                try
                {
                    while( rs.next() )
                    {
                        Object[] row = new Object[colIndices.size()];
                        // Populate row array
                        for( Map.Entry<String, Integer> col : colIndices.entrySet() )
                        {
                            row[col.getValue()] = rs.getObject( col.getKey() );
                        }

                        // Further process the row (add it to cache)
                        if( tableInfo.useBroadIndexing() )
                        {
                            broadCache.processRowWithBroadIndexing( colIndices, row, permutation );
                        }
                        if( tableInfo.useThinIndexing() )
                        {
                            thinCache.processRowWithThinIndexing( row );
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