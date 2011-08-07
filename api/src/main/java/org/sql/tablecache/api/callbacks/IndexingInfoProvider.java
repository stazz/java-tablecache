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

package org.sql.tablecache.api.callbacks;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.sql.tablecache.api.index.IndexingInfo;
import org.sql.tablecache.api.index.IndexingInfo.ThinIndexingInfo;
import org.sql.tablecache.api.table.TableInfo;
import org.sql.tablecache.api.table.TableRow;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public interface IndexingInfoProvider
{
    public Map<String, IndexingInfo> getIndexingInfo( TableInfo tableInfo );

    public final class DefaultIndexingInfoProvider
        implements IndexingInfoProvider
    {
        private static final Map<String, IndexingInfo> DEFAULT_INFO;
        static
        {
            Map<String, IndexingInfo> info = new HashMap<String, IndexingInfo>();
            info.put( null, new ThinIndexingInfo( new ThinIndexingPKProvider()
            {

                @Override
                public Object createThinIndexingKey( TableRow row )
                {
                    return row.get( row.getTableInfo().getPkColumns().iterator().next() );
                }
            } ) );

            DEFAULT_INFO = Collections.unmodifiableMap( info );
        }

        public static final IndexingInfoProvider INSTANCE = new DefaultIndexingInfoProvider();

        private DefaultIndexingInfoProvider()
        {
        }

        @Override
        public Map<String, IndexingInfo> getIndexingInfo( TableInfo tableInfo )
        {
            return DEFAULT_INFO;
        }
    }
}
