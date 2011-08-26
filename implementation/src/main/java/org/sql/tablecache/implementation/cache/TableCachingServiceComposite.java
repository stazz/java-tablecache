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

import java.util.HashMap;
import java.util.Map;

import org.qi4j.api.injection.scope.Structure;
import org.qi4j.api.mixin.Mixins;
import org.qi4j.api.object.ObjectBuilderFactory;
import org.qi4j.api.service.Activatable;
import org.qi4j.api.service.ServiceComposite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql.tablecache.api.cache.TableCache;
import org.sql.tablecache.api.cache.TableCachingService;

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

        private class TableCacheResultImpl
            implements TableCacheResult
        {
            private final boolean _created;
            private final TableCache _cache;

            public TableCacheResultImpl( TableCache cache, boolean created )
            {
                this._cache = cache;
                this._created = created;
            }

            @Override
            public TableCache cache()
            {
                return this._cache;
            }

            @Override
            public boolean created()
            {
                return this._created;
            }
        }

        private static final Logger LOGGER = LoggerFactory.getLogger( TableCachingServiceMixin.class );

        @Structure
        private ObjectBuilderFactory _obf;

        private Map<String, TableCache> _caches;

        private Object _cacheAccessLock;

        @Override
        public void activate()
            throws Exception
        {
            this._cacheAccessLock = new Object();
            this._caches = new HashMap<String, TableCache>();
        }

        @Override
        public void passivate()
            throws Exception
        {
            this._caches.clear();
            this._caches = null;
            this._cacheAccessLock = null;
        }

        @Override
        public TableCacheResult getOrCreateCache( String cacheID )
        {
            TableCache cache = null;
            boolean created = false;
            synchronized( this._cacheAccessLock )
            {
                cache = this._caches.get( cacheID );
                if( cache == null )
                {
                    cache = this._obf.newObjectBuilder( TableCacheImpl.class ).newInstance();
                    this._caches.put( cacheID, cache );
                    created = true;
                    LOGGER.info( "Created table cache with ID: " + cacheID + "." );
                }
            }

            return new TableCacheResultImpl( cache, created );
        }

        @Override
        public TableCache getCache( String cacheID )
        {
            TableCache cache = null;
            synchronized( this._cacheAccessLock )
            {
                cache = this._caches.get( cacheID );
                if( cache == null )
                {
                    throw new IllegalArgumentException( "The table cache with ID " + cacheID + " is not present." );
                }
            }
            return cache;
        }

        @Override
        public void removeCache( String cacheID )
        {
            synchronized( this._cacheAccessLock )
            {
                if( this._caches.remove( cacheID ) != null )
                {
                    LOGGER.info( "Removed table cache with ID: " + cacheID + "." );
                }
            }
        }

        @Override
        public Boolean hasCache( String cacheID )
        {
            synchronized( this._cacheAccessLock )
            {
                return this._caches.containsKey( cacheID );
            }
        }
    }

}