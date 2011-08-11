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

package org.sql.tablecache.api.index;

import java.util.Set;

import org.qi4j.api.util.NullArgumentException;
import org.sql.tablecache.api.callbacks.ThinIndexingKeyProvider;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public interface IndexingInfo
{
    public enum IndexType
    {
        THIN_PK,
        BROAD_PK,
        THIN
    }

    public IndexType getIndexType();

    public final class ThinPrimaryKeyIndexingInfo
        implements IndexingInfo
    {
        private final ThinIndexingKeyProvider _pkProvider;

        public ThinPrimaryKeyIndexingInfo( ThinIndexingKeyProvider provider )
        {
            super();
            NullArgumentException.validateNotNull( "thin indexing PK provider", provider );

            this._pkProvider = provider;
        }

        public ThinIndexingKeyProvider getPkProvider()
        {
            return this._pkProvider;
        }

        @Override
        public IndexType getIndexType()
        {
            return IndexType.THIN_PK;
        }
    }

    public final class BroadPrimaryKeyIndexingInfo
        implements IndexingInfo
    {
        private final Set<String> _indexingColumns;

        public BroadPrimaryKeyIndexingInfo( Set<String> indexingColumns )
        {
            super();
            this._indexingColumns = indexingColumns;
        }

        public Set<String> getIndexingColumns()
        {
            return this._indexingColumns;
        }

        @Override
        public IndexType getIndexType()
        {
            return IndexType.BROAD_PK;
        }
    }

    public final class ThinIndexingInfo
        implements IndexingInfo
    {
        private final ThinIndexingKeyProvider _keyProvider;

        public ThinIndexingInfo( ThinIndexingKeyProvider provider )
        {
            super();
            this._keyProvider = provider;
        }

        public ThinIndexingKeyProvider getKeyProvider()
        {
            return this._keyProvider;
        }

        @Override
        public IndexType getIndexType()
        {
            return IndexType.THIN;
        }
    }
}
