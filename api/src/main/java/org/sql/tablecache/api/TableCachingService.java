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

/**
 * The service to cache SQL tables located in some database. Each {@link TableCache} can cache one or more tables. This
 * {@link TableCachingService} acts as an aggregator for {@link TableCache}s. Most typical scenario is to make single
 * {@link TableCache} hold all tables of a certain schema. However, the string ID of each {@link TableCache} may not
 * necessarily be a schema name, and it is possible to fully customize the cache behaviour.
 * 
 * @author Stanislav Muhametsin
 * 
 */
public interface TableCachingService
{

    /**
     * Gets the table cache associated with given ID (usually schema name).
     * 
     * @param cacheID The ID of the cache.
     * @return The table cache with given ID.
     * @exception IllegalArgumentException If the cache with given ID is not found.
     */
    public TableCache getCache( String cacheID );

    /**
     * Gets the table cache associated with given ID (usually schema name). If the cache does not exist, an empty one
     * will be created.
     * 
     * @param cacheID The ID of the cache.
     * @return The table cache with given ID, or an empty table cache if the cache with given IS is not found.
     */
    public TableCache getOrCreateCache( String cacheID );

    /**
     * Removes the cache with given ID. Does nothing if the cache with given ID does not exist.
     * 
     * @param cacheID The ID of the cache to be removed.
     */
    public void removeCache( String cacheID );

}
