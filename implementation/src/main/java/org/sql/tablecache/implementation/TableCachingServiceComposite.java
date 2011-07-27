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

import java.util.HashMap;
import java.util.Map;

import org.qi4j.api.injection.scope.Structure;
import org.qi4j.api.mixin.Mixins;
import org.qi4j.api.object.ObjectBuilderFactory;
import org.qi4j.api.service.Activatable;
import org.qi4j.api.service.ServiceComposite;
import org.sql.tablecache.api.TableCache;
import org.sql.tablecache.api.TableCachingService;

@Mixins(
{
    TableCachingServiceComposite.TableCachingServiceMixin.class
})
public interface TableCachingServiceComposite
    extends TableCachingService, ServiceComposite
{

    public class TableCachingServiceMixin
        implements TableCachingService, Activatable
    {

        @Structure
        private ObjectBuilderFactory _obf;

        private Map<String, TableCache> _caches;

        @Override
        public void activate()
            throws Exception
        {
            this._caches = new HashMap<String, TableCache>();
        }

        @Override
        public void passivate()
            throws Exception
        {
            this._caches.clear();
            this._caches = null;
        }

        @Override
        public TableCache getOrCreateCache( String cacheID )
        {
            TableCache cache = this._caches.get( cacheID );
            if( cache == null )
            {
                cache = this._obf.newObjectBuilder( TableCacheImpl.class ).newInstance();
                this._caches.put( cacheID, cache );
            }
            return cache;
        }

        @Override
        public TableCache getCache( String cacheID )
        {
            TableCache cache = this._caches.get( cacheID );
            if( cache == null )
            {
                throw new IllegalArgumentException( "The table cache with ID " + cacheID + " is not present." );
            }
            return cache;
        }

        @Override
        public void removeCache( String cacheID )
        {
            this._caches.remove( cacheID );
        }
    }

}