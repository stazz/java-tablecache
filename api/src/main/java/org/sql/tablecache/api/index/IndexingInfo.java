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
import org.sql.tablecache.api.callbacks.ThinIndexingPKProvider;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public interface IndexingInfo
{
    public enum IndexType
    {
        THIN,
        BROAD
    }

    public IndexType getIndexType();

    public final class ThinIndexingInfo
        implements IndexingInfo
    {
        private final ThinIndexingPKProvider _pkProvider;

        public ThinIndexingInfo( ThinIndexingPKProvider provider )
        {
            super();
            NullArgumentException.validateNotNull( "thin indexing PK provider", provider );

            this._pkProvider = provider;
        }

        public ThinIndexingPKProvider getPkProvider()
        {
            return this._pkProvider;
        }

        @Override
        public IndexType getIndexType()
        {
            return IndexType.THIN;
        }
    }

    public final class BroadIndexingInfo
        implements IndexingInfo
    {
        private final Set<String> _indexingColumns;

        public BroadIndexingInfo( Set<String> indexingColumns )
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
            return IndexType.BROAD;
        }
    }
}
