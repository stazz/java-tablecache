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

package org.sql.tablecache.implementation.bootstrap;

import org.qi4j.api.common.Visibility;
import org.qi4j.bootstrap.Assembler;
import org.qi4j.bootstrap.AssemblyException;
import org.qi4j.bootstrap.ModuleAssembly;
import org.sql.tablecache.implementation.cache.TableCacheImpl;
import org.sql.tablecache.implementation.cache.TableCachingServiceComposite;

/**
 * 
 * @author 2011 Stanislav Muhametsin
 */
public class TableCacheAssembler
    implements Assembler
{

    public static final Visibility DEFAULT_VISIBILITY = Visibility.module;

    private final Visibility _visibility;

    public TableCacheAssembler()
    {
        this( DEFAULT_VISIBILITY );
    }

    public TableCacheAssembler( Visibility visibility )
    {
        this._visibility = visibility;
    }

    @Override
    public void assemble( ModuleAssembly module )
        throws AssemblyException
    {
        module.services( TableCachingServiceComposite.class ).visibleIn( this._visibility );
        module.objects( TableCacheImpl.class ).visibleIn( Visibility.module );
    }
}
